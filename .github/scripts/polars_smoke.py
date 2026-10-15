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
    LANDING_ITEM        the lakehouse holding the landed files (optional; the Files probes SKIP
                        without it)
    CSV_DIR             the daily CSV folder under it (default Files/csv_raw/daily)
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
# Where the AEMO daily CSVs sit under the landing item. The five legs land them in
# Files/csv_raw/daily (download_aemo.py); lakehouse_benchmark lands them in Files/csv.
CSV_DIR = (os.environ.get("CSV_DIR") or "Files/csv_raw/daily").strip("/")
# The archive log exists only in the five legs' landing zone.
HAS_ARCHIVE_LOG = CSV_DIR == "Files/csv_raw/daily"
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
    """One catalog, `onelake` (the Iceberg REST catalog, where models are written). Landed files
    are read with Polars SQL table functions, as the engine's models do: no source, so no
    `azure` catalog and no dbt-polars-catalog-azure."""
    secret = "{{ env_var('DBT_ENV_SECRET_ONELAKE_TOKEN') }}"
    onelake = {"name": "onelake", "type": "iceberg", "schema": SCHEMA,
               "pyiceberg_type": "rest", **ICEBERG_PROPS,
               "token": secret, "adls.token": secret}
    return {"polars_smoke": {"target": "smoke", "outputs": {"smoke": {
        "type": "polars", "threads": 1, "database": "onelake", "catalogs": [onelake]}}}}


PROJECT_YML = {"name": "polars_smoke", "version": "1.0.0", "profile": "polars_smoke",
               "model-paths": ["models"], "config-version": 2}


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
    "s_timestamp_with_tz": "select cast('2026-09-17 04:30:00' as timestamp with time zone) as parsed",
    "s_at_time_zone": "select cast('2026-09-17 04:30:00' as timestamp) at time zone 'UTC' as parsed",
    "s_generate_series": "select unnest(generate_series(cast('2026-01-01' as date), "
                         "cast('2026-01-10' as date), interval 1 day)) as date",
    "s_date_spine": "{{ dbt.date_spine('day', \"cast('2026-01-01' as date)\", "
                    "\"cast('2026-01-10' as date)\") }}",
    # pola-rs/polars#29876, #29877: the two SQL scoping bugs the polars models work around.
    "s_in_subquery_unqualified": "select a.k from (select 1 as k union all select 2 as k) as a "
                                 "where a.k in (select k from (select 1 as k) as b)",
    "s_with_in_derived": "select k from (with x as (select 1 as k) select k from x) as t",
    # dim_calendar as the DuckDB legs write it: generate_series in a derived table, no alias.
    "s_generate_series_derived": "select cast(date as date) as date from (select unnest("
                                 "generate_series(cast('2026-01-01' as date), "
                                 "cast('2026-01-03' as date), interval 1 day)) as date)",
    "s_window": "select k, row_number() over (partition by k % 2 order by v desc) as rn from "
                "(select 1 as k, 10 as v union all select 2 as k, 20 as v "
                "union all select 3 as k, 30 as v) as t",
    "s_decimal": "select cast(cast(2.5 as double) as decimal(18,6)) as dec18_6, "
                 "cast(cast(0.125 as double) as decimal(18,2)) as tie_break",
    "s_split_part": "select split_part(split_part('csv_raw/daily/PUBLIC_DAILY_1.CSV', '/', -1),"
                    " '.', 1) as file",
    "s_hash": "select {{ dbt.hash(\"'abc'\") }} as h",
}

# The fact models' pre_hook replaced by parse time: run_query reads the keys already in
# {{ this }} and renders them into the model as literals. No pre_hook, no merge_clauses — the
# two things dbt-polars does not do. Run 2 must leave key 1 alone and add key 3.
PICKER_SQL = incremental("append") + """
{%- set seen = [] -%}
{%- if execute and is_incremental() -%}
{%- set r = run_query("select distinct k from " ~ this) -%}
{%- if r is not none -%}{%- for row in r.rows -%}{%- do seen.append(row[0]) -%}{%- endfor -%}{%- endif -%}
{%- endif %}
select k, v from (
{% if var('batch') == 1 %}
  select 1 as k, 10 as v union all select 2 as k, 20 as v
{% else %}
  select 1 as k, 111 as v union all select 3 as k, 30 as v
{% endif %}
) as s
{% if seen %}where k not in ({{ seen | join(', ') }}){% endif %}
"""

# The ragged read the DuckDB legs do in duckdb_read_csv(), spelled as Polars SQL named
# arguments. Whether Polars SQL read_csv() takes ANY option is the question.
# `infer_schema_length => NULL` takes the width from the widest row, not the first: the padding
# the 131-column `schema` gives the sources (pola-rs/polars#29917).
READ_CSV_OPTIONS_SQL = ("has_header => false, skip_rows => 1, infer_schema => false, "
                        "infer_schema_length => NULL, truncate_ragged_lines => true, "
                        "include_file_paths => 'filename'")

# The ragged read as pola-rs/polars#29917 documents it for this case: the width from the widest
# row. try_parse_dates off, so the AEMO dates stay the strings the models parse.
READ_CSV_WIDE_SQL = ("has_header => false, skip_rows => 1, infer_schema_length => NULL, "
                     "try_parse_dates => false, include_file_paths => 'filename'")

RUN_QUERY_SQL = """
{%- if execute -%}
{%- set r = run_query("select 42 as n") -%}
{%- set n = (r.rows | length) if r is not none else -1 -%}
{%- set v = r.rows[0][0] if (r is not none and (r.rows | length) > 0) else -1 -%}
{%- else -%}{%- set n = -2 -%}{%- set v = -2 -%}{%- endif %}
select {{ n }} as rows_returned, {{ v }} as value
"""

DUNIT = "column_1 = 'D' and column_2 = 'DUNIT' and column_4 = '3'"
# The same record in a headerless Polars 2 read, which numbers its columns from column_0.
DUNIT0 = "column_0 = 'D' and column_1 = 'DUNIT' and column_3 = '3'"
# The same filter on inferred types: column_3 (the record version) reads as an integer.
DUNIT0_CAST = ("cast(column_0 as varchar) = 'D' and cast(column_1 as varchar) = 'DUNIT' "
               "and cast(column_3 as varchar) = '3'")


def glob_abfss():
    return f"abfss://{WORKSPACE}@{ONELAKE_DFS}/{LANDING_ITEM}/{CSV_DIR}/*.CSV"


def run_query_file_sql(abfss):
    return ("{%- if execute -%}\n"
            "{%- set r = run_query(\"select count(*) as n from read_csv('" + abfss + "')\") -%}\n"
            "{%- set n = r.rows[0][0] if (r is not none and (r.rows | length) > 0) else -1 -%}\n"
            "{%- else -%}{%- set n = -2 -%}{%- endif %}\n"
            "select {{ n }} as n\n")


def probes(ctx):
    files = bool(LANDING_ITEM)
    one = ctx.get("daily_file")
    two = ctx.get("daily_files") or []
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
        (8, "archive log parquet from Files, Polars SQL read_parquet()",
         "stg_csv_archive_log reads csv_raw_archive_log.parquet",
         {"f_archive_log": "{{ config(materialized='table') }}\nselect source_type, count(*) as n "
                           "from read_parquet('abfss://" + WORKSPACE + "@" + ONELAKE_DFS + "/"
                           + LANDING_ITEM + "/Files/csv_raw_archive_log.parquet') "
                           "group by source_type"}
         if files and HAS_ARCHIVE_LOG else None,
         [None], lambda c: expect_nonempty(c, "f_archive_log")),
        (9, "landed AEMO CSV, Polars SQL read_csv()",
         "a model reading a named file directly, the way the DuckDB legs call read_csv()",
         {"f_read_csv": "{{ config(materialized='table') }}\nselect count(*) as n from read_csv('"
                        + (one or {}).get("abfss", "") + "')"}
         if one else None,
         [None], lambda c: expect_nonempty(c, "f_read_csv")),
        (10, "landed AEMO CSV, Polars SQL read_csv() with options",
         "the ragged read duckdb_read_csv() does: no header, all String, ragged rows, filename",
         {"f_read_csv_opts": "{{ config(materialized='table') }}\nselect filename, count(*) as n "
                             "from read_csv('" + (one or {}).get("abfss", "") + "', "
                             + READ_CSV_OPTIONS_SQL + ") where " + DUNIT0 + " group by filename"}
         if one else None,
         [None], lambda c: expect_dunit(c, "f_read_csv_opts")),
        (11, "landed AEMO CSV, Polars SQL read_csv() defaults",
         "which columns a bare read_csv() gives a ragged file: is the DUNIT record addressable",
         {"f_read_csv_shape": "{{ config(materialized='table') }}\nselect * from read_csv('"
                              + (one or {}).get("abfss", "") + "') limit 20"}
         if one else None,
         [None], lambda c: expect_shape(c, "f_read_csv_shape")),
        (12, "landed AEMO CSV, Polars SQL read_csv() over a glob",
         "the folder at once, bypassing the source's lower-case .csv (dbt-polars#29)",
         {"f_read_csv_glob": "{{ config(materialized='table') }}\nselect * from read_csv('"
                             + glob_abfss() + "') limit 5"}
         if files else None,
         [None], lambda c: expect_nonempty(c, "f_read_csv_glob")),
        (13, "landed AEMO CSV, a file list as UNION ALL",
         "how a list rendered at parse time would be read: one read_csv() per file",
         {"f_read_csv_list": "{{ config(materialized='table') }}\nselect count(*) as n from ("
                             + " union all ".join(f"select 1 as x from read_csv('{f['abfss']}')"
                                                  for f in two) + ") as t"}
         if len(two) >= 2 else None,
         [None], lambda c: expect_total_rows(c, "f_read_csv_list", two)),
        (14, "file picker at parse time, against {{ this }}",
         "the pre_hook replaced: run_query lists the keys present, the model excludes them",
         {"m_picker": PICKER_SQL}, [{"batch": 1}, {"batch": 2}],
         lambda c: expect_rows(c, "m_picker", INSERT_ONLY_EXPECTED)),
        (15, "run_query over a landed file at parse time",
         "the fact models count pending files before they read; does that context reach OneLake",
         {"f_run_query_file": "{{ config(materialized='table') }}\n"
                              + run_query_file_sql((one or {}).get("abfss", ""))}
         if one else None,
         [None], lambda c: expect_positive(c, "f_run_query_file")),
    ]
    # dbt's own relationships test SQL, on keys where exactly one child has no parent. On the
    # polars engine every daily fct_scada row was reported orphaned while fct_summary joined
    # the same DUIDs fine -- is it the LEFT JOIN ... IS NULL on the right side?
    out.append((len(out) + 1, "dbt relationships SQL (LEFT JOIN, right side IS NULL)",
                "the generic test's anti-join: one orphan in three",
                {"m_rel_child": "{{ config(materialized='table') }}\nselect 'A' as k union all "
                                "select 'B' as k union all select 'C' as k",
                 "m_rel_parent": "{{ config(materialized='table') }}\nselect 'A' as k union all "
                                 "select 'B' as k",
                 "m_rel_orphans": "{{ config(materialized='table') }}\n"
                                  "with child as (select k as from_field from {{ ref('m_rel_child') }} "
                                  "where k is not null), parent as (select k as to_field from "
                                  "{{ ref('m_rel_parent') }}) select from_field from child left join "
                                  "parent on child.from_field = parent.to_field "
                                  "where parent.to_field is null"},
                [None], lambda c: expect_rows(c, "m_rel_orphans", {("C",)})))
    for name, sql in SHAPES.items():
        out.append((len(out) + 1, f"SQL shape: {name[2:]}", "Polars SQL dialect, as the models write it",
                    {name: "{{ config(materialized='table') }}\n" + sql}, [None],
                    (lambda n: lambda c: expect_nonempty(c, n))(name)))
    # How the fact models would read with read_csv() options instead of the padded source: the
    # folder glob, narrowed to the files the picker chose, DUNIT rows counted per file.
    out.append((len(out) + 1, "landed AEMO CSV, read_csv() with options over the folder, "
                "filtered to a file list",
                "the padded source replaced by Polars SQL read_csv() (pola-rs/polars#29917)",
                {"f_read_csv_opts_list": "{{ config(materialized='table') }}\nselect filename, "
                                         "count(*) as n from read_csv('" + glob_abfss() + "', "
                                         + READ_CSV_OPTIONS_SQL + ") where "
                                         "split_part(filename, '/', -1) in ("
                                         + ", ".join(f"'{f['path'].rsplit('/', 1)[1]}'"
                                                     for f in two)
                                         + ") and " + DUNIT0 + " group by filename"}
                if len(two) >= 2 else None,
                [None], lambda c: expect_dunit(c, "f_read_csv_opts_list", two)))
    # THE ISSUE'S FORM (pola-rs/polars#29917): no header and infer_schema_length => NULL, so the
    # width is the widest row's. Not infer_schema => false, which sets the inference length to
    # 0 and so the first line's width; not truncate_ragged_lines, which drops the extra fields.
    # Types are inferred, so the record filter casts. Graded on WIDTH too: column_52 must exist
    # and be filled on the DUNIT rows, which a 25-wide read cannot do.
    # ONE read_csv() PER FILE, joined by UNION ALL: over the folder glob, the whole-file
    # inference ran past the step's 20 minutes before the filename filter applied (run
    # 38653690208). The fact models render the picker's list the same way.
    for key, files in (("f_read_csv_wide", [one] if one else []), ("f_read_csv_wide_list", two)):
        reads = " union all ".join(
            "select filename, column_52 as c52 from read_csv('" + f["abfss"] + "', "
            + READ_CSV_WIDE_SQL + ") where " + DUNIT0_CAST for f in files)
        out.append((len(out) + 1,
                    "landed AEMO CSV, read_csv() widest-row read"
                    + (", one per file, UNION ALL" if key.endswith("list") else ""),
                    "has_header => false, infer_schema_length => NULL: every DUNIT field kept",
                    {key: "{{ config(materialized='table') }}\nselect * from (" + reads + ") as t"}
                    if (len(files) >= (2 if key.endswith("list") else 1)) else None,
                    [None], (lambda k, f: lambda c: expect_dunit_wide(c, k, f))(
                        key, files if key.endswith("list") else None)))
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


def padded_scan(abfss):
    """The file read by Polars directly, padded to the widest AEMO record: what a correct read
    gives. A narrower read silently drops rows."""
    import polars as pl
    return pl.scan_csv(abfss, has_header=False, skip_rows=1,
                       schema={f"column_{i}": pl.String for i in range(1, 132)},
                       truncate_ragged_lines=True, missing_columns="insert",
                       storage_options={"bearer_token": TOKEN})


def expect_dunit(ctx, name, files=None):
    import polars as pl
    rows, _ = table_rows(ctx, name)
    if not rows:
        return "FAIL", "table is empty"
    got = sum(r["n"] for r in rows)
    truth = sum(padded_scan(f["abfss"])
                .filter((pl.col("column_1") == "D") & (pl.col("column_2") == "DUNIT")
                        & (pl.col("column_4") == "3"))
                .select(pl.len()).collect().item()
                for f in (files or [ctx["daily_file"]]))
    detail = (f"DUNIT rows {got} from {len(rows)} file(s), a padded read gives {truth}, "
              f"filename={rows[0].get('filename')}")
    return ("PASS" if got == truth else "FAIL"), detail


def expect_dunit_by_position(ctx, name, files=None):
    """For a model that copies a headerless read as-is (`select *`). Grades by POSITION, so
    it does not depend on what Polars names the columns -- run 38120589486's DUNIT filter on
    column_1/2/4 matched nothing, and its glob row had column_2 = 'CASESOLUTION', which reads
    like 0-based names. Also reports the width: a DUNIT record is 53 fields, and a read that
    takes its width from the first line cuts it short."""
    import polars as pl
    import pyarrow.compute as pc
    arrow = ctx["catalog"].load_table((SCHEMA, name)).scan().to_arrow()
    cols = [c for c in arrow.schema.names if c != "filename"]
    if not arrow.num_rows or len(cols) < 4:
        return "FAIL", f"{arrow.num_rows} rows, columns {cols[:6]}"
    mask = pc.and_(pc.and_(pc.equal(arrow[cols[0]], "D"), pc.equal(arrow[cols[1]], "DUNIT")),
                   pc.equal(arrow[cols[3]], "3"))
    got = pc.sum(mask).as_py() or 0
    truth = sum(padded_scan(f["abfss"])
                .filter((pl.col("column_1") == "D") & (pl.col("column_2") == "DUNIT")
                        & (pl.col("column_4") == "3"))
                .select(pl.len()).collect().item()
                for f in (files or [ctx["daily_file"]]))
    if len(cols) >= 53:
        last = pc.filter(arrow[cols[52]], mask)
        full = f"53rd field non-null on {len(last) - last.null_count} of {len(last)} DUNIT rows"
    else:
        full = "DUNIT rows CUT SHORT of their 53 fields"
    detail = (f"DUNIT rows {got}, a padded read gives {truth}; {len(cols)} columns named "
              f"{cols[0]}..{cols[-1]}; {full}")
    return ("PASS" if got == truth and len(cols) >= 53 else "FAIL"), detail


def expect_dunit_wide(ctx, name, files=None):
    """DUNIT rows counted against the padded read, and their 53rd field filled."""
    import polars as pl
    rows, _ = table_rows(ctx, name)
    truth = sum(padded_scan(f["abfss"])
                .filter((pl.col("column_1") == "D") & (pl.col("column_2") == "DUNIT")
                        & (pl.col("column_4") == "3"))
                .select(pl.len()).collect().item()
                for f in (files or [ctx["daily_file"]]))
    filled = sum(1 for r in rows if r["c52"] is not None)
    detail = (f"DUNIT rows {len(rows)}, a padded read gives {truth}; 53rd field filled on "
              f"{filled}")
    return ("PASS" if rows and len(rows) == truth and filled == len(rows) else "FAIL"), detail


def expect_total_rows(ctx, name, files):
    """Every row of every file, against the padded reads summed."""
    import polars as pl
    rows, _ = table_rows(ctx, name)
    if not rows:
        return "FAIL", "table is empty"
    got = rows[0]["n"]
    truth = sum(padded_scan(f["abfss"]).select(pl.len()).collect().item() for f in files)
    return ("PASS" if got == truth else "FAIL"), f"{got} rows from {len(files)} files, padded reads give {truth}"


def expect_shape(ctx, name):
    """Reports the columns Polars chose. PASS means only that it read; the detail is the answer."""
    tbl = ctx["catalog"].load_table((SCHEMA, name))
    arrow = tbl.scan().to_arrow()
    if not arrow.num_rows:
        return "FAIL", "table is empty"
    cols = arrow.schema.names
    return "PASS", f"{len(cols)} columns {cols[:8]}{' ...' if len(cols) > 8 else ''}, first={arrow.slice(0, 1).to_pylist()[0]}"


def expect_positive(ctx, name):
    rows, _ = table_rows(ctx, name)
    n = rows[0]["n"] if rows else None
    return ("PASS" if isinstance(n, int) and n > 0 else "FAIL"), f"run_query returned n={n}"


def write_project(root, models):
    (root / "dbt_project.yml").write_text(json.dumps(PROJECT_YML), encoding="utf-8")
    (root / "profiles.yml").write_text(json.dumps(profile_yml()), encoding="utf-8")
    mdir = root / "models"
    shutil.rmtree(mdir, ignore_errors=True)
    mdir.mkdir()
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


def newest_daily_files(n=2):
    """The n newest landed PUBLIC_DAILY files, from the OneLake DFS listing. [] on any failure,
    which makes the per-file probes SKIP: an empty landing zone is not a finding about Polars."""
    if not LANDING_ITEM:
        return []
    import urllib.parse
    import urllib.request
    q = urllib.parse.urlencode({"resource": "filesystem", "recursive": "false",
                                "directory": f"{LANDING_ITEM}/{CSV_DIR}",
                                "maxResults": "5000"})
    req = urllib.request.Request(f"https://{ONELAKE_DFS}/{WORKSPACE}?{q}",
                                 headers={"Authorization": f"Bearer {TOKEN}"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            paths = json.load(r).get("paths", [])
    except Exception as e:  # noqa: BLE001 - reported, then SKIP
        say(f"    (could not list {CSV_DIR}: {oneline(e)})")
        return []
    names = sorted((p["name"] for p in paths if not p.get("isDirectory")
                    and p["name"].lower().endswith(".csv")), reverse=True)
    if not names:
        say(f"    (no CSVs under {CSV_DIR} -- has anything landed?)")
    return [{"path": name, "stem": name.rsplit("/", 1)[1].rsplit(".", 1)[0],
             "abfss": f"abfss://{WORKSPACE}@{ONELAKE_DFS}/{name}"} for name in names[:n]]


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
            continue
        try:  # a git install records where it came from; PyPI installs have no direct_url
            from importlib.metadata import distribution
            url = json.loads(distribution(pkg).read_text("direct_url.json") or "{}")
            if url:
                say(f"{'':26}   from {url.get('url')} @ {url.get('vcs_info', {}).get('commit_id')}")
        except Exception:  # noqa: BLE001
            pass
    say(f"{'warehouse':26} {WAREHOUSE}")
    say(f"{'namespace':26} {SCHEMA}")
    say(f"{'landing item':26} {LANDING_ITEM or '(none -- Files probes SKIP)'} {CSV_DIR}")


def main():
    banner()
    daily_files = newest_daily_files()
    ctx = {"catalog": iceberg_catalog(), "daily_files": daily_files,
           "daily_file": daily_files[0] if daily_files else None}
    try:
        ctx["catalog"].create_namespace_if_not_exists((SCHEMA,))
    except Exception as e:  # noqa: BLE001 - dbt-polars creates it too; its own error is the finding
        say(f"    (namespace pre-create: {oneline(e, 200)})")
    drop_probe_tables(ctx["catalog"])
    daily = ctx["daily_file"]
    say(f"{'daily file':26} {daily['path'] if daily else '(none)'}")

    results = []
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for number, name, why, models, runs, check in probes(ctx):
            say(f"\n[{number:2}] {name}\n     why: {why}")
            if models is None and runs != ["debug"]:
                results.append((number, name, "SKIP", "not in this landing zone"))
                say("     SKIP")
                continue
            write_project(root, models or {})
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
