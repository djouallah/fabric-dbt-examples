"""Probe whether dbt-polars can run this repo's SQL models against OneLake.

NOT AN ENGINE. This is a smoke test for a candidate sixth engine, and its output is meant to
be read by a human — or pasted into an upstream issue. It never fails the workflow: a red
result IS the deliverable.

WHY IT EXISTS. docs/candidate-engines.md said Polars had no dbt adapter to point a --target
at. dbt-polars (datamindedbe/incubator-dbt-polars) now exists on PyPI, on dbt-core 1.x. It
runs a SQL model through Polars' own SQL engine (`pl.SQLContext`), and writes Iceberg through
pyiceberg. Reading its source says: merge is a delete and then an append, hooks raise an
error, `run_query` has no database behind it, a source is a file under a catalog path. Read
from source is not the same as run, so this script runs it.

SQL MODELS ONLY. The gold layer is SQL on all five engines; a model rewritten as a Python
(dataframe) model would prove something about the rewrite, not about the engine. No probe
writes a `.py` model, and tests_py/test_polars_smoke.py pins that.

HOW. It writes a throwaway dbt project into a temp dir, and each probe puts only its own
models in it and runs `dbt` in-process (dbtRunner). A probe that wrote a table is checked by
reading the table back with pyiceberg: "dbt said OK" is not accepted as "it worked".

WHAT IT MAY WRITE, AND WHERE. Tables in the POLARS_SMOKE_SCHEMA namespace only (default
polars_smoke), dropped at the start and, unless POLARS_SMOKE_KEEP=1, at the end. Never a
namespace the five engines use.

Env (the workflow supplies all of these):
    WAREHOUSE_PATH      "{workspace_id}/{dbt lakehouse id}", the iceberg leg's warehouse
    ONELAKE_TOKEN       the https://storage.azure.com/ token
    LANDING_ITEM        the dbt_landing lakehouse id (optional; the Files probes SKIP without it)
    POLARS_SMOKE_SCHEMA namespace to build in
    POLARS_SMOKE_KEEP   "1" leaves the tables behind

Usage:
    python .github/scripts/polars_smoke.py
"""

import json
import os
import shutil
import sys
import tempfile
import traceback
from pathlib import Path

WAREHOUSE = os.environ["WAREHOUSE_PATH"]
TOKEN = os.environ["ONELAKE_TOKEN"]
LANDING_ITEM = os.environ.get("LANDING_ITEM", "")
SCHEMA = os.environ.get("POLARS_SMOKE_SCHEMA") or "polars_smoke"
KEEP = os.environ.get("POLARS_SMOKE_KEEP") == "1"
WORKSPACE = WAREHOUSE.split("/", 1)[0]

ICEBERG_ENDPOINT = "https://onelake.table.fabric.microsoft.com/iceberg"
ONELAKE_BLOB = "onelake.blob.fabric.microsoft.com"
ONELAKE_DFS = "onelake.dfs.fabric.microsoft.com"

# The five engines write <engine>_landing / <engine>_mart, and dbo always exists. A probe that
# drops tables must never be pointed at any of them.
if SCHEMA == "dbo" or SCHEMA.endswith(("_landing", "_mart")):
    sys.exit(f"refusing to use namespace {SCHEMA!r}: it belongs to a real leg")

# The token reaches dbt only through a DBT_ENV_SECRET_ variable, which dbt scrubs from its
# logs. AZURE_STORAGE_TOKEN is for probe 9: a Polars SQL read_csv() takes no credentials, so
# the environment is the only way one could reach it.
os.environ["DBT_ENV_SECRET_ONELAKE_TOKEN"] = TOKEN
os.environ["AZURE_STORAGE_TOKEN"] = TOKEN
os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")

# The pyiceberg properties the iceberg leg's REST catalog needs, as the bench uses them:
# `token` signs the catalog calls, `adls.token` the data files. The profile below and the
# read-back catalog use the same set.
ICEBERG_PROPS = {
    "uri": ICEBERG_ENDPOINT,
    "warehouse": WAREHOUSE,
    "adls.account-name": "onelake",
    "adls.account-host": ONELAKE_BLOB,
}


def profile_yml():
    """Two catalogs: `onelake` (the Iceberg REST catalog, where models are written) and
    `landing` (the dbt_landing lakehouse's Files, where sources are read)."""
    secret = "{{ env_var('DBT_ENV_SECRET_ONELAKE_TOKEN') }}"
    onelake = {"name": "onelake", "type": "iceberg", "schema": SCHEMA,
               "pyiceberg_type": "rest", **ICEBERG_PROPS,
               "token": secret, "adls.token": secret}
    catalogs = [onelake]
    if LANDING_ITEM:
        # The azure catalog builds az://<container>/<prefix>/<schema>/<identifier>.<format>.
        # OneLake: account `onelake`, container = the workspace, and use_fabric_endpoint so
        # object_store goes to *.fabric.microsoft.com instead of *.core.windows.net.
        catalogs.append({
            "name": "landing", "type": "azure", "schema": "Files",
            "account_name": "onelake", "container": WORKSPACE, "prefix": LANDING_ITEM,
            "credentials": {"storage_options": {
                "account_name": "onelake", "bearer_token": secret,
                "use_fabric_endpoint": "true"}},
        })
    return {"polars_smoke": {"target": "smoke", "outputs": {"smoke": {
        "type": "polars", "threads": 1, "database": "onelake", "catalogs": catalogs}}}}


PROJECT_YML = {"name": "polars_smoke", "version": "1.0.0", "profile": "polars_smoke",
               "model-paths": ["models"], "config-version": 2}

# AEMO's DUNIT record is 53 columns and a PUBLIC_DAILY file also holds wider records (DREGION
# is 130). The source reads with no header; `infer_schema: false` reads every column as String,
# which is what all_varchar does on the DuckDB legs.
CSV_READ_OPTIONS = {
    "has_header": False,
    "skip_rows": 1,
    "infer_schema": False,
    "truncate_ragged_lines": True,
    "include_file_paths": "filename",
    "ignore_errors": True,
}


def sources_yml(daily_stem):
    if not LANDING_ITEM:
        return None
    tables_files = [{"name": "archive_log", "identifier": "csv_raw_archive_log",
                     "config": {"file_format": "parquet"}}]
    tables_daily = [{"name": "daily_glob", "identifier": "*",
                     "config": {"file_format": "csv", "read_options": CSV_READ_OPTIONS}}]
    if daily_stem:
        tables_daily.append({"name": "daily_one", "identifier": daily_stem,
                             "config": {"file_format": "csv",
                                        "read_options": CSV_READ_OPTIONS}})
    return {"version": 2, "sources": [
        {"name": "landing_files", "database": "landing", "schema": "Files",
         "tables": tables_files},
        {"name": "landing_daily", "database": "landing", "schema": "Files/csv_raw/daily",
         "tables": tables_daily},
    ]}


# ---- the probes ------------------------------------------------------------------------------
# Each probe: (number, name, why, models, runs, check).
#   models  {model_name: sql}. Only these are in models/ while the probe runs.
#   runs    the --vars for each `dbt run`, in order. [None] means one run, no vars.
#   check   a function(ctx) -> (status, detail) run after the last dbt run, or None when the
#           dbt result alone is the answer.

def incremental(strategy, keys=None, extra=""):
    key = f", unique_key={keys!r}" if keys else ""
    return (f"{{{{ config(materialized='incremental', incremental_strategy='{strategy}'"
            f"{key}{extra}) }}}}\n")


MERGE_SQL = incremental("merge", ["k1", "k2", "k3"]) + """
{% if var('batch') == 1 %}
select 1 as k1, 1 as k2, 1 as k3, 10 as v
union all select 2 as k1, 2 as k2, 2 as k3, 20 as v
{% else %}
select 2 as k1, 2 as k2, 2 as k3, 99 as v
union all select 3 as k1, 3 as k2, 3 as k3, 30 as v
{% endif %}
"""
MERGE_EXPECTED = {(1, 1, 1, 10), (2, 2, 2, 99), (3, 3, 3, 30)}

# What every fact model in dbt1/ asks for: a merge that never updates a matched row.
INSERT_ONLY_SQL = incremental(
    "merge", ["k"], ", merge_clauses={'when_matched': [{'action': 'do_nothing'}]}") + """
{% if var('batch') == 1 %}
select 1 as k, 10 as v union all select 2 as k, 20 as v
{% else %}
select 1 as k, 111 as v union all select 3 as k, 30 as v
{% endif %}
"""
INSERT_ONLY_EXPECTED = {(1, 10), (2, 20), (3, 30)}

# The other way to only add rows: an append that filters out the keys already there.
ANTI_JOIN_SQL = incremental("append") + """
select k, v from (
{% if var('batch') == 1 %}
  select 1 as k, 10 as v union all select 2 as k, 20 as v
{% else %}
  select 1 as k, 111 as v union all select 3 as k, 30 as v
{% endif %}
) as s
{% if is_incremental() %}
where k not in (select k from {{ this }})
{% endif %}
"""

# The SQL shapes the models use, one table model each, so one failure cannot hide another.
SHAPES = {
    "s_slash_date_strptime": "select strptime('2026/09/17 04:30:00', '%Y/%m/%d %H:%M:%S') as parsed",
    "s_slash_date_cast": "select cast('2026/09/17 04:30:00' as timestamp) as parsed",
    "s_timestamptz": "select cast('2026-09-17 04:30:00' as timestamptz) as parsed",
    "s_generate_series": "select unnest(generate_series(cast('2026-01-01' as date), "
                         "cast('2026-01-10' as date), interval 1 day)) as date",
    "s_date_spine": "{{ dbt.date_spine('day', \"cast('2026-01-01' as date)\", "
                    "\"cast('2026-01-10' as date)\") }}",
    "s_window": "select k, row_number() over (partition by k % 2 order by v desc) as rn from "
                "(select 1 as k, 10 as v union all select 2 as k, 20 as v "
                "union all select 3 as k, 30 as v) as t",
    "s_decimal": "select cast(cast(2.5 as double) as decimal(18,6)) as dec18_6, "
                 "cast(cast(0.125 as double) as decimal(18,2)) as tie_break",
    "s_split_part": "select split_part(split_part('csv_raw/daily/PUBLIC_DAILY_1.CSV', '/', -1),"
                    " '.', 1) as file",
    "s_hash": "select {{ dbt.hash(\"'abc'\") }} as h",
}

RUN_QUERY_SQL = """
{%- if execute -%}
{%- set r = run_query("select 42 as n") -%}
{%- set n = (r.rows | length) if r is not none else -1 -%}
{%- set v = r.rows[0][0] if (r is not none and (r.rows | length) > 0) else -1 -%}
{%- else -%}{%- set n = -2 -%}{%- set v = -2 -%}{%- endif %}
select {{ n }} as rows_returned, {{ v }} as value
"""

DUNIT = "column_1 = 'D' and column_2 = 'DUNIT' and column_4 = '3'"


def probes(ctx):
    files = bool(LANDING_ITEM)
    one = ctx.get("daily_file")
    out = [
        (1, "dbt debug", "does the profile load and the adapter connect", None, ["debug"], None),
        (2, "table model, built twice",
         "a table model into the Iceberg catalog; the second build must replace, not append",
         {"m_table": "{{ config(materialized='table') }}\n"
                     "select 1 as k, 10 as v union all select 2 as k, 20 as v"},
         [None, None], lambda c: expect_rows(c, "m_table", {(1, 10), (2, 20)})),
        (3, "incremental merge, three-column key",
         "fct_summary and fct_price merge on several columns. Run 2 updates one key and adds "
         "one; a rebuild instead of a merge would lose key 1",
         {"m_merge": MERGE_SQL}, [{"batch": 1}, {"batch": 2}],
         lambda c: expect_rows(c, "m_merge", MERGE_EXPECTED)),
        (4, "insert-only merge (merge_clauses do_nothing)",
         "what every fact model asks for: a matched row is left alone",
         {"m_insert_only": INSERT_ONLY_SQL}, [{"batch": 1}, {"batch": 2}],
         lambda c: expect_rows(c, "m_insert_only", INSERT_ONLY_EXPECTED)),
        (5, "append + NOT IN {{ this }}",
         "the other insert-only shape (dim_calendar's filter)",
         {"m_anti_join": ANTI_JOIN_SQL}, [{"batch": 1}, {"batch": 2}],
         lambda c: expect_rows(c, "m_anti_join", INSERT_ONLY_EXPECTED)),
        (6, "pre_hook",
         "the fact models choose their files in a pre_hook",
         {"m_hook": "{{ config(materialized='table', pre_hook='select 1') }}\nselect 1 as k"},
         [None], lambda c: expect_rows(c, "m_hook", {(1,)})),
        (7, "run_query at parse time",
         "the fact models ask the archive log which files are pending with run_query",
         {"m_run_query": "{{ config(materialized='table') }}\n" + RUN_QUERY_SQL},
         [None], lambda c: expect_rows(c, "m_run_query", {(1, 42)})),
        (8, "archive log parquet from Files, as a source",
         "stg_csv_archive_log reads csv_raw_archive_log.parquet",
         {"f_archive_log": "{{ config(materialized='table') }}\nselect source_type, count(*) as n "
                           "from {{ source('landing_files', 'archive_log') }} group by source_type"}
         if files else None,
         [None], lambda c: expect_nonempty(c, "f_archive_log")),
        (9, "landed AEMO CSV, Polars SQL read_csv()",
         "a model reading a named file directly, the way the DuckDB legs call read_csv()",
         {"f_read_csv": "{{ config(materialized='table') }}\nselect count(*) as n from read_csv('"
                        + (one or {}).get("abfss", "") + "')"}
         if one else None,
         [None], lambda c: expect_nonempty(c, "f_read_csv")),
        (10, "landed AEMO CSV, source by exact file name",
         "one landed PUBLIC_DAILY file, DUNIT record, with the file name column",
         {"f_daily_one": "{{ config(materialized='table') }}\nselect filename, count(*) as n "
                         "from {{ source('landing_daily', 'daily_one') }} where " + DUNIT
                         + " group by filename"}
         if one else None,
         [None], lambda c: expect_dunit(c, "f_daily_one")),
        (11, "landed AEMO CSV, source over the folder (identifier '*')",
         "every file in csv_raw/daily at once",
         {"f_daily_glob": "{{ config(materialized='table') }}\nselect count(distinct filename) "
                          "as files, count(*) as n from {{ source('landing_daily', 'daily_glob') }}"
                          " where " + DUNIT}
         if files else None,
         [None], lambda c: expect_nonempty(c, "f_daily_glob")),
    ]
    for name, sql in SHAPES.items():
        out.append((len(out) + 1, f"SQL shape: {name[2:]}", "Polars SQL dialect, as the models write it",
                    {name: "{{ config(materialized='table') }}\n" + sql}, [None],
                    (lambda n: lambda c: expect_nonempty(c, n))(name)))
    return out


# ---- running dbt and reading back -----------------------------------------------------------

def scrub(text):
    return str(text).replace(TOKEN, "***")


def say(text=""):
    print(scrub(text), flush=True)


def oneline(e, limit=600):
    s = " ".join(scrub(e).split())
    return s if len(s) <= limit else s[:limit] + " ..."


def iceberg_catalog():
    from pyiceberg.catalog import load_catalog
    return load_catalog("readback", **ICEBERG_PROPS, token=TOKEN, **{"adls.token": TOKEN})


def table_rows(ctx, name):
    tbl = ctx["catalog"].load_table((SCHEMA, name))
    rows = tbl.scan().to_arrow().to_pylist()
    ops = [s.summary.operation.value if s.summary else "?" for s in tbl.snapshots()]
    return rows, ops


def expect_rows(ctx, name, expected):
    rows, ops = table_rows(ctx, name)
    got = {tuple(r.values()) for r in rows}
    detail = f"rows={sorted(got)} snapshots={ops}"
    if got == expected and len(rows) == len(expected):
        return "PASS", detail
    return "FAIL", f"expected {sorted(expected)}, got {detail}"


def expect_nonempty(ctx, name):
    rows, ops = table_rows(ctx, name)
    if not rows:
        return "FAIL", f"table is empty, snapshots={ops}"
    return "PASS", f"{len(rows)} rows, first={rows[0]}"


def expect_dunit(ctx, name):
    """Compare with the same file read by Polars directly, padded to the widest AEMO record:
    the count a correct read gives. A narrower read silently drops DUNIT rows."""
    import polars as pl
    rows, _ = table_rows(ctx, name)
    if not rows:
        return "FAIL", "table is empty"
    got = sum(r["n"] for r in rows)
    truth = (pl.scan_csv(ctx["daily_file"]["abfss"], has_header=False, skip_rows=1,
                         schema={f"column_{i}": pl.String for i in range(1, 132)},
                         truncate_ragged_lines=True, missing_columns="insert",
                         storage_options={"bearer_token": TOKEN})
             .filter((pl.col("column_1") == "D") & (pl.col("column_2") == "DUNIT")
                     & (pl.col("column_4") == "3"))
             .select(pl.len()).collect().item())
    detail = f"DUNIT rows {got}, a padded read gives {truth}, filename={rows[0]['filename']}"
    return ("PASS" if got == truth else "FAIL"), detail


def write_project(root, models, sources):
    (root / "dbt_project.yml").write_text(json.dumps(PROJECT_YML), encoding="utf-8")
    (root / "profiles.yml").write_text(json.dumps(profile_yml()), encoding="utf-8")
    mdir = root / "models"
    shutil.rmtree(mdir, ignore_errors=True)
    mdir.mkdir()
    if sources:
        (mdir / "_sources.yml").write_text(json.dumps(sources), encoding="utf-8")
    for name, sql in (models or {}).items():
        (mdir / f"{name}.sql").write_text(sql, encoding="utf-8")


def dbt(root, command, vars_):
    """One dbt invocation, in-process. Returns (ok, message)."""
    from dbt.cli.main import dbtRunner
    args = [command, "--project-dir", str(root), "--profiles-dir", str(root)]
    if vars_:
        args += ["--vars", json.dumps(vars_)]
    res = dbtRunner().invoke(args)
    if res.exception is not None:
        return False, oneline(res.exception)
    if command == "debug":
        return bool(res.success), "" if res.success else "dbt debug reported failure"
    bad = [f"{r.node.name}: {oneline(r.message)}" for r in res.result.results
           if str(r.status) not in ("success", "pass")]
    if bad:
        return False, "; ".join(bad)
    if not res.result.results:
        return False, "dbt ran nothing"
    return True, ""


def newest_daily_file():
    """The newest landed PUBLIC_DAILY file, from the OneLake DFS listing. None on any failure,
    which makes the per-file probes SKIP: an empty landing zone is not a finding about Polars."""
    if not LANDING_ITEM:
        return None
    import urllib.parse
    import urllib.request
    q = urllib.parse.urlencode({"resource": "filesystem", "recursive": "false",
                                "directory": f"{LANDING_ITEM}/Files/csv_raw/daily",
                                "maxResults": "5000"})
    req = urllib.request.Request(f"https://{ONELAKE_DFS}/{WORKSPACE}?{q}",
                                 headers={"Authorization": f"Bearer {TOKEN}"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            paths = json.load(r).get("paths", [])
    except Exception as e:  # noqa: BLE001 - reported, then SKIP
        say(f"    (could not list csv_raw/daily: {oneline(e)})")
        return None
    names = sorted((p["name"] for p in paths if not p.get("isDirectory")
                    and p["name"].lower().endswith(".csv")), reverse=True)
    if not names:
        say("    (no CSVs under Files/csv_raw/daily -- has a real leg ever landed?)")
        return None
    name = names[0]
    base = name.rsplit("/", 1)[1]
    return {"path": name, "stem": base.rsplit(".", 1)[0],
            "abfss": f"abfss://{WORKSPACE}@{ONELAKE_DFS}/{name}"}


def drop_probe_tables(cat):
    """Drops tables in SCHEMA, and only there (see the guard at the top)."""
    try:
        for ident in cat.list_tables((SCHEMA,)):
            cat.drop_table(ident)
    except Exception as e:  # noqa: BLE001 - a missing namespace is fine
        say(f"    (cleanup: {oneline(e, 200)})")


def banner():
    from importlib.metadata import version
    for pkg in ("dbt-core", "dbt-polars", "dbt-polars-catalog-azure", "polars", "pyiceberg"):
        try:
            say(f"{pkg:26} {version(pkg)}")
        except Exception:  # noqa: BLE001
            say(f"{pkg:26} (not installed)")
    say(f"{'warehouse':26} {WAREHOUSE}")
    say(f"{'namespace':26} {SCHEMA}")
    say(f"{'landing item':26} {LANDING_ITEM or '(none -- Files probes SKIP)'}")


def main():
    banner()
    ctx = {"catalog": iceberg_catalog(), "daily_file": newest_daily_file()}
    try:
        ctx["catalog"].create_namespace_if_not_exists((SCHEMA,))
    except Exception as e:  # noqa: BLE001 - dbt-polars creates it too; its own error is the finding
        say(f"    (namespace pre-create: {oneline(e, 200)})")
    drop_probe_tables(ctx["catalog"])
    daily = ctx["daily_file"]
    say(f"{'daily file':26} {daily['path'] if daily else '(none)'}")
    sources = sources_yml(daily["stem"] if daily else None)

    results = []
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for number, name, why, models, runs, check in probes(ctx):
            say(f"\n[{number:2}] {name}\n     why: {why}")
            if models is None and runs != ["debug"]:
                results.append((number, name, "SKIP", "no landed files"))
                say("     SKIP")
                continue
            write_project(root, models or {}, sources)
            status, detail = "PASS", ""
            for i, vars_ in enumerate(runs):
                command = "debug" if vars_ == "debug" else "run"
                ok, msg = dbt(root, command, None if command == "debug" else vars_)
                if not ok:
                    status, detail = "FAIL", f"dbt {command} #{i + 1}: {msg}"
                    break
            if status == "PASS" and check:
                try:
                    status, detail = check(ctx)
                except Exception as e:  # noqa: BLE001 - a failed read-back is a result
                    status, detail = "FAIL", f"read-back: {oneline(e)}"
            results.append((number, name, status, detail))
            say(f"     {status} {detail}")

    if not KEEP:
        drop_probe_tables(ctx["catalog"])

    say("\n==== SUMMARY ====")
    for number, name, status, detail in results:
        say(f"[{number:2}] {status:4} {name}" + (f" -- {detail}" if detail else ""))


if __name__ == "__main__":
    try:
        main()
    except Exception:  # noqa: BLE001 - the probe never fails the job
        say(traceback.format_exc())
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)
