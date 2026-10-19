"""Compact the OneLake Iceberg catalog's data files (post-load maintenance).

TWO ENGINES WRITE ICEBERG into that one catalog: `iceberg` (dbt-duckdb) and `polars`
(dbt-polars, through pyiceberg). Both append a data file per table per run and neither
compacts, so this runs over both, each under its own schemas (COMPACT_ENGINES, below).

Every model the iceberg leg WRITES is an insert-only incremental merge — that is deliberate
(OneLake accepts one add-snapshot per commit), but it means each dbt run appends another
small data file per table and nothing ever folds them back together. This runs
iceberg_rewrite_data_files() over each table, folding files below MIN_FILE_SIZE up toward
TARGET_FILE_SIZE.

ITS JOB IS TO MERGE, NEVER TO SPLIT, and that takes saying so explicitly — see the size
constants below for the run where leaving the bounds defaulted cost 2.8 GB of rewrite to make
a layout worse.

iceberg_rewrite_data_files landed in duckdb/iceberg#1035 and is not in a stable
duckdb release yet, so the compact job (pipeline.yml) installs the latest PRE-release duckdb
(`--pre duckdb`, unpinned) and nothing else. The iceberg extension binary is keyed to the
duckdb build, so whatever pip resolves brings its own matching extension; connect() prints
both shas.
has_rewrite_function() checks for the function rather than assuming it, so a resolution
that happens to lack it degrades to "nothing to compact" instead of failing the job.

Ported from djouallah/analytics-as-code scripts/compact_iceberg.py, which runs this against
an R2-backed catalog. What differs here:
  - No S3 uploader tuning. The reference sets s3_uploader_max_parts_per_file for R2's
    "non-trailing parts must be equal length" rule; OneLake writes go through the azure
    extension over abfss:// and never touch the S3 uploader.

AN EXPLICIT STORAGE SECRET, AGAIN -- credential vending stopped working. The ATTACH token
authorises the REST catalog; the data and metadata files are read with an `azure` secret on
the same OneLake token (provider access_token), the way dbt1/profiles.yml's iceberg output
already carries one for Files/. Vending is left on (no ACCESS_DELEGATION_MODE), so it takes
over again when the regression is fixed.
  - Runs 38992461895 and 38992730624 (duckdb v2.0.0-alpha48624 / ffe0b8b7a2, iceberg extension
    10eafd0b34) failed EVERY table on its first metadata read: "AzureStorageFileSystem open
    file '.../metadata/snap-....avro' failed with code 'Unauthorized'". The vended
    `adls.sas-token.onelake.dfs.fabric.microsoft.com` credential (usable since
    duckdb/duckdb-iceberg#1331) is no longer applied -- a duckdb regression, nothing that
    changed here. The compact job installs an unpinned `--pre duckdb`, so it is the first to
    see one.
  - History: the secret was removed on 2026-09-17, together with ACCESS_DELEGATION_MODE
    'none' and a priming iceberg_metadata() scan for duckdb/iceberg#1349, once run
    35234519405 rewrote fct_scada 62 -> 3 cold on vended credentials alone. Take the secret
    out again only with a run that proves vending works without it.

Known limitations of the upstream function:
  - manifest-level column statistics are not populated for rewritten files
  - V3 tables and partition spec evolution are unsupported
  - there is no snapshot expiry, so the pre-compaction data files stay in OneLake until
    something expires them. Reads get faster immediately; storage does not shrink.

Never fails the pipeline: every table is best-effort and errors are printed, not raised, so
a bad run just means the tables stay fragmented until the next one.

Usage:
    python .github/scripts/compact_iceberg.py
"""

import os
import sys
import time

import duckdb

# The OneLake Iceberg REST catalog. One endpoint for every tenant and workspace; the
# warehouse path below is what picks the lakehouse.
ENDPOINT = "https://onelake.table.fabric.microsoft.com/iceberg"
TOKEN = os.environ["ONELAKE_TOKEN"]
# The catalog's warehouse is "<workspace id>/<lakehouse id>". Composed here from the two
# parts rather than handed over whole: the `land` job passes the lakehouse GUID as a job output,
# and GitHub drops an output that contains a secret -- the workspace id is one. From `land` and
# not from `build` because `build` is a matrix, whose entries overwrite one another's outputs.
WAREHOUSE = f"{os.environ['FABRIC_WORKSPACE_ID']}/{os.environ['DATA_LAKEHOUSE_ID']}"

# THE SIZE A MERGE AIMS FOR -- and on its own it is NOT a "leave big files alone" setting.
# iceberg_rewrite_data_files bin-packs toward the target the way Spark's rewrite_data_files
# does, so when min/max are left to their defaults it brackets the target (0.75x and 1.8x) and
# rewrites everything OUTSIDE that band -- which means it SPLITS files above ~1.8x as
# enthusiastically as it merges files below 0.75x.
#
# That cost a real run. 35350150404 folded 3,000 archive files in one go and left a good
# layout -- fct_scada at 7 files of ~410 MB. Compaction then spent 2,872 MB of rewrite turning
# those 7 into 43 files of ~67 MB, i.e. straight down onto a 64MiB target. fct_summary went
# 2 -> 12 and fct_price 2 -> 7 the same way. Nothing was broken; the job just undid a better
# layout than it produced, and paid OneLake write traffic to do it.
#
# So all three bounds are explicit now. MAX is the one that matters: a file above the target is
# there because a big batch wrote it, and Direct Lake would rather have it whole.
TARGET_FILE_SIZE = "256MiB"
# Below this, fold. Deliberately just under the target rather than at 64MiB, so the 43-file
# tables the old setting produced are themselves candidates and get folded back up.
MIN_FILE_SIZE = "192MiB"
# Above this, split -- set high enough that nothing a fold writes ever is. 2GiB clears the
# largest single file seen here by a wide margin (fct_scada's were ~410 MB after a 3,000-file
# batch), so in practice it reads as "never split", while staying a real ceiling rather than a
# sentinel the function might reject.
MAX_FILE_SIZE = "2GiB"
# Don't bother rewriting a table that only has a handful of files. Also what keeps
# already-tidy tables (dim_calendar, anything compacted last run) cheap.
#
# Overridable because a dbt run adds exactly one data file per table, so in steady state the
# tables sit well under this and compaction is a no-op — correct, but it means the rewrite
# path goes untested for weeks. Dropping this to 2 on a manual run forces a real rewrite and
# proves the catalog still accepts the commit. Leave the default alone.
MIN_INPUT_FILES = int(os.environ.get("COMPACT_MIN_INPUT_FILES", "5"))
# Stop starting new tables past this much wall clock, so the job reports what it did instead
# of being killed by the runner's timeout mid-rewrite. Keep it well under the workflow's
# timeout-minutes. Whatever gets skipped is picked up next run — compaction is incremental
# by nature.
BUDGET_MINUTES = float(os.environ.get("COMPACT_BUDGET_MINUTES", "70"))

# DuckDB's azure extension reads/writes OneLake over a curl transport (its default transport
# fails the OneLake TLS handshake). dbt sets this via on-run-start in dbt_project.yml; this
# script is not a dbt run, so it sets it itself. Same default, same env var.
AZURE_TRANSPORT = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE", "default")

# WHICH ENGINES. The Iceberg writers built this run, space-separated, from the dispatch input
# (pipeline.yml). Anything that is not an Iceberg writer is ignored rather than guessed at.
ICEBERG_WRITERS = ("iceberg", "polars")
ENGINES = [e for e in os.environ.get("COMPACT_ENGINES", "iceberg polars").split()
           if e in ICEBERG_WRITERS]

# The schemas are resolved the SAME WAY macros/generate_schema_name.sql resolves them, and
# must stay in step with it: this script is not a dbt run, so nothing else would catch a
# drift. All five engines share one lakehouse, so each writer's tables live under
# `<engine>_landing` / `<engine>_mart`; compacting a bare `landing`/`mart` would either find
# nothing (and report a tidy catalog, wrongly) or reach into another engine's schema.
_DBT_SCHEMA = os.environ.get("DBT_SCHEMA", "mart")


def schemas(engine):
    prefix = engine if _DBT_SCHEMA == "mart" else f"{_DBT_SCHEMA}_{engine}"
    return f"{prefix}_landing", f"{prefix}_mart"


# Hand-ordered, not discovered — we know the models, and listing them costs a metadata scan
# per table for nothing. A new model just gets added here.
#
# Ordered by expected fragmentation and by what we'd least regret dropping if the budget runs
# out: the dashboard-facing tables first (they take an append every run and are what Power BI
# reads), then the small dimensions, then the big historical facts last — those are the
# slowest to rewrite and the least sensitive to small-file overhead. Engines interleave per
# model, so a spent budget costs both the same tables rather than one engine all of its own.
#
# This is the canonical set. `fct_summary_daily` used to be listed here and is NOT a model
# in this repo — it existed only in the iceberg source repo, which is exactly the one-engine
# drift the merge exists to stop. Listing it only bought a "no version-hint" read failure
# every run.
# NO stg_csv_archive_log, on either engine. On iceberg it is not a catalog table: the model
# declares database='memory', so it is a view in DuckDB's in-memory catalog that dies with the
# run -- the same session view duckrun and ducklake get (it WAS an insert-only table there
# until 2026-09-18, so an old one may still sit in OneLake; nothing below will touch it). On
# polars it is a `table` model, replaced whole every run: one file, nothing to fold.
MODELS = [
    ("landing", "fct_price_today"),
    ("landing", "fct_scada_today"),
    ("mart", "fct_summary"),
    ("mart", "dim_calendar"),
    ("mart", "dim_duid"),
    ("landing", "fct_price"),
    ("landing", "fct_scada"),
]
TABLES = [f"{schemas(e)[0 if zone == 'landing' else 1]}.{name}"
          for zone, name in MODELS for e in ENGINES]


def oneline(e):
    """Collapse a duckdb error to its first line — they carry a SQL echo and a caret ruler."""
    return " ".join(str(e).split("\n")[0].split())


def connect():
    con = duckdb.connect(":memory:")
    # Plain install first. The 1.6.0/2.0.0 dev line self-identifies as v2.0.0-alpha*, and
    # nightly-extensions.duckdb.org has no iceberg build under that version — asking
    # core_nightly first just buys a 404 and a scary log line. The core extension for these
    # builds does carry iceberg_rewrite_data_files.
    try:
        con.install_extension("iceberg")
    except Exception as e:
        print(f"  (core install failed, trying core_nightly: {e})", flush=True)
        con.execute("FORCE INSTALL iceberg FROM core_nightly")
    con.load_extension("iceberg")
    # Say exactly what was loaded. `duckdb.__version__` is the wheel; the iceberg binary is
    # fetched at runtime from the extension repo and can be newer (or older) than the wheel,
    # and its extension_version is the duckdb-iceberg git sha it was built from -- the one
    # fact that says whether an upstream fix is in this job or not.
    try:
        ver = con.execute("PRAGMA version").fetchone()
        ext = con.execute(
            "SELECT extension_version, installed_from, install_mode FROM duckdb_extensions() "
            "WHERE extension_name = 'iceberg'"
        ).fetchone()
        print(f"engine {ver[0]} ({ver[1]}); iceberg extension {ext[0]} from {ext[1] or 'core'} "
              f"({ext[2]})", flush=True)
    except Exception as e:  # noqa: BLE001 -- diagnostics only
        print(f"(could not read engine/extension versions: {oneline(e)})", flush=True)

    con.execute(f"SET GLOBAL azure_transport_option_type = '{AZURE_TRANSPORT}'")
    con.execute("SET GLOBAL temp_directory = '/tmp/duckdb_spill'")

    # The storage credential, explicitly: vended credentials stopped being applied (runs
    # 38992461895, 38992730624 -- see the module docstring). Unscoped, like the iceberg
    # profile's, so it covers every abfss:// path the rewrite opens.
    con.execute(
        f"CREATE SECRET onelake_storage (TYPE azure, PROVIDER access_token, "
        f"ACCESS_TOKEN '{TOKEN}')"
    )
    # The token on the ATTACH authenticates to the REST catalog.
    con.execute(
        f"ATTACH '{WAREHOUSE}' AS onelake "
        f"(TYPE iceberg, ENDPOINT '{ENDPOINT}', TOKEN '{TOKEN}')"
    )
    return con


def catalog_tables(con):
    """What the catalog actually holds, as {"schema.table"}.

    One cheap REST list call, not a metadata scan. Worth it: the first run reported
    mart.fct_summary_daily as a "Failed to read iceberg table / no version-hint" error, which
    is what you get when the name doesn't resolve as a catalog table and falls back to being
    read as a path. Listing up front says plainly whether a table is missing or misnamed
    instead of dressing it up as a read failure.
    """
    try:
        rows = con.execute(
            "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'onelake'"
        ).fetchall()
        return {f"{s}.{t}" for s, t in rows}
    except Exception as e:
        print(f"  (could not list catalog tables: {oneline(e)})", flush=True)
        return None


def has_rewrite_function(con):
    return (
        con.execute(
            "SELECT count(*) FROM duckdb_functions() "
            "WHERE function_name = 'iceberg_rewrite_data_files'"
        ).fetchone()[0]
        > 0
    )


def compact(con, table, say):
    """Compact one table. Returns (table, status) for the report.

    One call, cold: no prior read of the table, no metadata scan. The rewrite reads the
    manifests itself to decide what is below the target, so a "0 rewritten" here still
    proves the catalog handed over working storage credentials.
    """
    fq = f"onelake.{table}"

    say("rewriting")
    try:
        row = con.execute(
            f"SELECT rewritten_data_files, added_data_files, rewritten_bytes "
            f"FROM iceberg_rewrite_data_files('{fq}', "
            f"target_file_size_bytes => '{TARGET_FILE_SIZE}', "
            f"min_file_size_bytes => '{MIN_FILE_SIZE}', "
            f"max_file_size_bytes => '{MAX_FILE_SIZE}', "
            f"min_input_files => {MIN_INPUT_FILES})"
        ).fetchone()
    except Exception as e:
        # Keep it to one line — the full multi-line duckdb error is already on stdout above.
        return (table, f"ERROR: {type(e).__name__}: {oneline(e)}")

    # Report what the function actually returned. "No row at all" and "a row of zeros" are
    # different failure modes and both look like a tidy table if you collapse them into one
    # "skipped" — which is exactly how the first run hid that nothing was happening.
    if row is None:
        return (table, "NO ROW returned")

    rewritten, added, rewritten_bytes = row
    if not rewritten:
        # Either under the file threshold or every file already inside [MIN, MAX] -- nothing
        # distinguishes the two without a metadata scan, and neither is a problem. This is the
        # EXPECTED line for a table a big batch has just written: those files are above MIN and
        # nowhere near MAX, so there is nothing to do and that is the point.
        return (table, f"0 rewritten — under the {MIN_INPUT_FILES}-file threshold, or every "
                       f"file already between {MIN_FILE_SIZE} and {MAX_FILE_SIZE}")

    mb = (rewritten_bytes or 0) / 1048576.0
    return (table, f"OK ({rewritten} -> {added} files, {mb:.1f} MB)")


def report(lines, duckdb_version):
    """THE JOB LOG, and deliberately not $GITHUB_STEP_SUMMARY. This ran before `layout` and so
    used to be the first table on the run page -- one engine's maintenance, ahead of the
    cross-engine layout table that is the point of the whole repo. Compaction is a detail of the
    iceberg leg; it belongs in that leg's log. Nothing else reads this."""
    out = ["=" * 100, f"Iceberg compaction (duckdb {duckdb_version}; "
                      f"engines: {', '.join(ENGINES) or 'none'})", "-" * 100]
    for table, status in lines:
        out.append(f"{table:<34}{status}")
    out.append("=" * 100)
    print("\n".join(out))


def main():
    version = duckdb.__version__
    print(f"duckdb {version} — compacting {len(TABLES)} table(s) toward {TARGET_FILE_SIZE}, "
          f"folding below {MIN_FILE_SIZE}, splitting above {MAX_FILE_SIZE}, "
          f"min_input_files={MIN_INPUT_FILES}, budget={BUDGET_MINUTES:g}min, in order:")
    for t in TABLES:
        print(f"  - onelake.{t}")
    print(flush=True)

    if not TABLES:
        print("no Iceberg writer among COMPACT_ENGINES -- nothing to compact")
        return

    con = connect()
    if not has_rewrite_function(con):
        print(
            f"iceberg_rewrite_data_files() not available in duckdb {version} — the pinned "
            "build or its iceberg extension has moved. Nothing compacted."
        )
        return

    present = catalog_tables(con)
    if present is not None:
        print(f"catalog holds {len(present)} table(s): {', '.join(sorted(present))}")
        missing = [t for t in TABLES if t not in present]
        if missing:
            print(f"::warning::not in the catalog, will be skipped: {', '.join(missing)}")
        print(flush=True)

    started = time.monotonic()
    total = len(TABLES)
    lines = []
    for i, table in enumerate(TABLES, 1):
        if present is not None and table not in present:
            lines.append((table, "NOT IN CATALOG — never built, or renamed"))
            continue

        elapsed = (time.monotonic() - started) / 60.0
        if elapsed >= BUDGET_MINUTES:
            # Never drop tables silently — say which ones and why.
            for skipped in TABLES[i - 1:]:
                lines.append((skipped,
                              f"not attempted ({BUDGET_MINUTES:g}min budget spent)"))
            print(f"time budget spent after {elapsed:.1f}min — not attempting: "
                  f"{', '.join(TABLES[i - 1:])}", flush=True)
            break

        prefix = f"[{i}/{total}] onelake.{table}"

        def say(phase, _prefix=prefix):
            # Which step it's on, so a slow table can't be mistaken for a hang.
            print(f"{_prefix} ... {phase}", flush=True)

        print(f"{prefix} ... ({elapsed:.1f}min elapsed)", flush=True)
        _, status = compact(con, table, say)
        took = (time.monotonic() - started) / 60.0 - elapsed
        print(f"{prefix}: {status}  [{took:.1f}min]\n", flush=True)
        lines.append((table, status))

    report(lines, version)
    con.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Maintenance must never fail the pipeline.
        print(f"compact_iceberg failed (non-fatal): {e}", file=sys.stderr)
