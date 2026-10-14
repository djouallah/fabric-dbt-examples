#!/usr/bin/env python3
"""The dbt commands ONE engine runs, WHEREVER that engine runs.

    python .github/scripts/run_dbt.py <duckrun | iceberg | ducklake | spark | polars>
        [--no-fingerprint] [--fingerprint-out PATH]

THE TWO MODES RUN THE SAME SEQUENCE -- build, the resume loop, a guarded `dbt retry`, the
parity fingerprint -- so a local leg and a Fabric run are the same run on different hardware,
not two implementations that agree by inspection:

  * local: pipeline.yml's "dbt build on this runner" step (or a laptop).
  * fabric: the deployed `run` notebook (fabric_items/run.Notebook), started by the deployed
    pipeline.

It used to be run_in_fabric.py, and the runner ran a bare `dbt build || dbt retry` instead.
That bare form is why ducklake could not go local at all: its catalog is an auto-pausing
Fabric SQL DB, the first connection after a pause is SQL 40613, and dbt dies at the ATTACH
with no run_results.json for `dbt retry` to heal. See RESUME_SIGNATURE below.

NOTHING HERE ASKS WHERE IT IS RUNNING. The environment is the caller's business -- see the
DUCKDB_TEMP_DIR note below -- so there is no "am I in Fabric" test to get wrong.

EVERY ENGINE IS dbt-core 1.x, INVOKED IN-PROCESS through dbtRunner. In-process is what lets
spark's session (spark_session.py) be built before dbt picks it up, and res.exception gives a
real traceback in the log. There used to be a second path -- iceberg spent a day on
dbt OSS 2, a Rust engine with no dbtRunner to import, launched as a subprocess out of a
separate dbt2/ project. Both are gone with it.

THE FILE NAME MUST NOT START WITH `dbt_`. dbt 1.x's plugin manager imports every importable
module whose name starts with dbt_ when a command starts, and this script's own directory is
sys.path[0] -- so as dbt_in_fabric.py it was imported as a "plugin" during `dbt build` and
re-executed itself recursively inside dbt's own invoke.

In Fabric it runs as a subprocess of the notebook kernel with cwd = the unpacked repo, and the
tokens are already in the environment (provision.py emits them there, minted by tokens.py from
notebookutils); on the runner pipeline.yml mints them into $GITHUB_ENV. Either way the config
arrives as env vars.
"""
import os
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# engine -> project directory. EVERY engine: tests_py/test_modes.py asserts it matches
# _layout.PROJECT_OF -- an engine missing here reaches no build at all, silent and green.
PROJECT = {"duckrun": "dbt1", "iceberg": "dbt1", "ducklake": "dbt1", "spark": "dbt1",
           "polars": "dbt1"}

argv = sys.argv[1:]
# DEFAULT ON, and the polarity is load-bearing: both modes fingerprint in THIS process (the
# runner and the notebook pass --fingerprint-out to keep it as a file). An opt-IN flag would
# mean a caller who forgets it publishes no fingerprint at all -- and a missing
# history/parity/<engine>.json is invisible, because parity.py compare just reports it has
# fewer than two to compare and exits 0. Forgetting an opt-OUT costs one redundant query.
fingerprint = "--no-fingerprint" not in argv
# --fingerprint-out PATH: ALSO write the fingerprint JSON to PATH. The Fabric mode's notebook
# passes a lakehouse path, which is how CI reads a Fabric run's fingerprint back
# (fabric_run.py) -- there is no streamed log to lift it out of.
fingerprint_out = argv[argv.index("--fingerprint-out") + 1] if "--fingerprint-out" in argv else ""
# Resolved NOW: the script chdir's into the project below, and a path given relative to the
# caller's cwd (pipeline.yml passes history/parity/<engine>.json) must not move with it.
fingerprint_out = str(Path(fingerprint_out).resolve()) if fingerprint_out else ""
engine = next((a for i, a in enumerate(argv)
               if not a.startswith("--") and (i == 0 or argv[i - 1] != "--fingerprint-out")), "")
if engine not in PROJECT:
    sys.exit(f"usage: run_dbt.py [{' | '.join(PROJECT)}] [--no-fingerprint] "
             f"[--fingerprint-out PATH]")
project_dir = REPO / PROJECT[engine]

# DuckDB spill. THE CALLER OWNS THIS: pipeline.yml sets DUCKDB_TEMP_DIR at job level for a
# runner leg; in Fabric it is derived from TMPDIR, which the `run` notebook puts on the
# notebook's ~135 GiB work disk (/tmp is a ~19 GiB overlay). setdefault is the whole branch.
# The duckrun and iceberg on-run-start hooks read it.
os.environ.setdefault(
    "DUCKDB_TEMP_DIR", os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb_spill")
)
os.makedirs(os.environ["DUCKDB_TEMP_DIR"], exist_ok=True)

# POLARS SQL read_csv() TAKES NO CREDENTIALS. The polars fact models read the landed CSVs with
# it (macros/polars_read_csv.sql), and Polars reaches OneLake from them only through
# AZURE_STORAGE_TOKEN. It is ONELAKE_TOKEN, which both modes already put in the environment.
if engine == "polars" and os.environ.get("ONELAKE_TOKEN"):
    os.environ.setdefault("AZURE_STORAGE_TOKEN", os.environ["ONELAKE_TOKEN"])

# NOTHING IS POPPED OUT OF THE ENVIRONMENT HERE. This script used to delete
# AZURE_TRANSPORT_OPTION_TYPE and CURL_CA_INFO as belt and braces -- inside Fabric DuckDB's
# default OneLake transport is the one that works -- which is the OPPOSITE of true on a GitHub
# runner, where pipeline.yml sets both at job level and DuckDB fails the OneLake TLS handshake
# without them ("Problem with the SSL CA cert (path? access rights?)"). The Fabric notebook
# never sets them.

BASE = ["--target", engine, "--profiles-dir", "."]


# The text of the last failure, for RESUMING below.
LAST_ERROR = ""


def run(*args, callbacks=None) -> bool:
    """dbt-core 1.x, in-process. cwd is the project dir, which is how dbt finds the project."""
    global LAST_ERROR
    from dbt.cli.main import dbtRunner  # imported late: after the env is set

    print(f"=== dbt {' '.join(args)} ({engine}) ===", flush=True)
    t0 = time.time()
    res = dbtRunner(callbacks=callbacks or []).invoke([*args, *BASE])
    print(f"=== dbt {args[0]}: success={res.success} in {int(time.time() - t0)}s ===", flush=True)
    LAST_ERROR = "" if res.success else str(res.exception or "")
    if res.exception:
        print(res.exception, flush=True)
    return bool(res.success)


# spark: the SparkSession dbt-spark's `method: session` will getOrCreate() -- jars, Delta,
# OneLake auth and the already-written tables registered. Built here because none of it can be
# added once the JVM is up; spark_session.py says why each piece is there.
if engine == "spark":
    sys.path.insert(0, str(REPO / ".github" / "scripts"))
    import spark_session  # noqa: E402

    spark_session.start()

# dbtRunner has no --project-dir equivalent that also moves the profiles lookup, and
# --profiles-dir . is relative, so put the process in the project.
os.chdir(project_dir)

# A PAUSED SERVERLESS DATABASE IS NOT A BUILD FAILURE -- WAIT FOR IT.
#
# ducklake's catalog is a Fabric SQL DB, which auto-pauses when idle. The first connection
# after that gets SQL error 40613 ("Database ... is not currently available. Please retry the
# connection later"), WHICH IS THE SERVER ASKING US TO RETRY: the resume is already under way
# and takes about a minute. dbt has no notion of this and dies at the ATTACH, in ~26 seconds,
# before a single model runs -- and `dbt retry` below cannot help, because a crash at
# connection open writes no run_results.json to retry from. Observed on run 35294987062, the
# first ducklake run after a 12-hour gap; every earlier run was close enough behind the last
# that the database was still awake.
#
# Only this signature, and only the FIRST build: any other failure is a real one and must
# stay fast. Retrying the whole `dbt build` rather than probing the database first is
# deliberate -- the probe that matters is the one dbt itself makes, with its own connection,
# its own token and its own extension.
RESUME_SIGNATURE = "40613"
RESUME_ATTEMPTS = 5
RESUME_WAIT_SECONDS = 60

ok = run("build")
for attempt in range(1, RESUME_ATTEMPTS + 1):
    if ok or RESUME_SIGNATURE not in LAST_ERROR:
        break
    print(f"=== catalog database is resuming (SQL {RESUME_SIGNATURE}); waiting "
          f"{RESUME_WAIT_SECONDS}s and rebuilding, attempt {attempt}/{RESUME_ATTEMPTS} ===",
          flush=True)
    time.sleep(RESUME_WAIT_SECONDS)
    ok = run("build")

if not ok and (project_dir / "target" / "run_results.json").exists():
    # Same as the workflow's `|| dbt retry`: heals the tail of a run that lost a few nodes.
    # Only when the build got far enough to write run_results.json -- a crash at connection
    # open leaves nothing to retry, and `dbt retry` then just fails a second time.
    ok = run("retry")
if not ok:
    sys.exit(1)

if not fingerprint:
    sys.exit(0)
# Last, and only after a green build above: the run-operation reads the gold TABLE, not the run, so
# taken after a failure it reports the previous run's numbers and parity.py would grade this
# run on them.
# ONE in-process run-operation, with a callback that keeps what the macro log()s, so it can be
# written to --fingerprint-out as well. In-process matters: spark's session (its jars, its
# OneLake auth, its registered tables) lives in THIS process, and a `dbt` subprocess would
# start a JVM with none of it.
logged: list[str] = []


def keep_log(event) -> None:
    if event.info.name.startswith("JinjaLog"):
        logged.append(event.info.msg)


if not run("run-operation", "parity_fingerprint", callbacks=[keep_log]):
    sys.exit(1)
if fingerprint_out:
    import json  # noqa: E402

    sys.path.insert(0, str(REPO / ".github" / "scripts"))
    import parity  # noqa: E402

    fp = parity.extract("\n".join(logged))
    if fp is None:
        sys.exit("fingerprint failed: no JSON in the run-operation's log")
    Path(fingerprint_out).parent.mkdir(parents=True, exist_ok=True)
    Path(fingerprint_out).write_text(json.dumps(fp, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {fingerprint_out}: {fp['rows_total']:,} rows, {fp['duids']} DUIDs, "
          f"{fp['days']} days", flush=True)
