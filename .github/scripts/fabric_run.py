#!/usr/bin/env python3
"""Run ONE engine in Fabric: start the deployed `run_pipeline` for it, wait, and read its
parity fingerprint back. The second half of the Fabric mode (deploy.py is the first); the
local mode is run_dbt.py on the runner.

    python .github/scripts/fabric_run.py <duckrun | iceberg | ducklake | polars>

The pipeline runs the deployed `run` notebook with `engine`, `run_id` and land=False (CI landed
once already, in the `land` job; two runs landing at once race on the archive log). The notebook
runs provision.py, run_dbt.py and writes the fingerprint to Files/dbt_runs/<run_id>/<engine>.json
in the shared `dbt` lakehouse; this script copies it to history/parity/<engine>.json, where the
local mode's fingerprint step writes, so `parity.py compare` grades both modes the same way.

The leg's compute is the notebook item: it is recorded as the leg's compute GUID, the way
measure_cu.py attributes capacity units to a run and an engine. EVERY fabric leg shares that
one GUID, so the leg's WINDOW must be the pipeline job instance's own start and end -- written
here from Fabric's timestamps -- and never the GitHub job's: the matrix legs start together and
wait while the pipeline (concurrency 1) runs them one after another, so their GitHub windows
overlap completely and each would be credited with every other leg's notebook CU.

Exit 1 when the pipeline run does not succeed or the fingerprint is missing.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))

ENGINES = ("duckrun", "iceberg", "ducklake", "polars")
PIPELINE = "run_pipeline"
NOTEBOOK = "run"
POLL_SECONDS = 30
TIMEOUT_SECONDS = 3 * 3600


def _utc(stamp) -> str | None:
    """Fabric's `2026-10-12T03:01:02.1234567` (UTC) in record.now()'s shape, or None."""
    if not stamp:
        return None
    from datetime import datetime, timezone

    head = str(stamp).rstrip("Z").split(".")[0]
    return datetime.fromisoformat(head).replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> int:
    engine = sys.argv[1] if len(sys.argv) > 1 else ""
    if engine not in ENGINES:
        sys.exit(f"usage: fabric_run.py [{' | '.join(ENGINES)}]")
    import onelake
    import provision
    import record

    ws = provision.WS
    run_id = os.environ.get("GITHUB_RUN_ID") or str(int(time.time()))
    pid = provision.find("dataPipelines", PIPELINE)
    nb = provision.find("notebooks", NOTEBOOK)
    if not pid or not nb:
        sys.exit(f"{PIPELINE} / {NOTEBOOK} are not deployed; run deploy.py (items) first")
    record.item(nb, "compute", "Notebook", NOTEBOOK, engine=engine)
    record.leg(engine, compute=[nb])

    r = provision.req("POST", f"workspaces/{ws}/items/{pid}/jobs/instances?jobType=Pipeline",
                      json={"executionData": {"parameters": {
                          "engine": engine, "run_id": run_id, "land": False}}})
    if r.status_code != 202:
        sys.exit(f"could not start {PIPELINE}: {r.status_code} {r.text[:500]}")
    loc = r.headers.get("Location", "")
    instance = loc.rstrip("/").rsplit("/", 1)[-1]
    print(f"started {PIPELINE} for {engine}: job instance {instance}", flush=True)

    status, t0 = "", time.time()
    while time.time() - t0 < TIMEOUT_SECONDS:
        time.sleep(POLL_SECONDS)
        j = provision.req("GET", f"workspaces/{ws}/items/{pid}/jobs/instances/{instance}").json()
        status = j.get("status", "")
        print(f"  {int(time.time() - t0)}s: {status}", flush=True)
        if status in ("Completed", "Failed", "Cancelled", "Deduped"):
            break
    record.leg(engine, started=_utc(j.get("startTimeUtc")), finished=_utc(j.get("endTimeUtc")),
               outcome="success" if status == "Completed" else (status or "timeout").lower())
    if status != "Completed":
        reason = (j.get("failureReason") or {}) if isinstance(j, dict) else {}
        sys.exit(f"{PIPELINE} for {engine} ended {status or 'TIMED OUT'}: "
                 f"{json.dumps(reason)[:1000]}")

    data_id = provision.find("lakehouses", provision.DATA_LAKEHOUSE)
    base = f"{provision.abfss(data_id, 'Files')}/dbt_runs/{run_id}"
    import obstore

    try:
        body = bytes(obstore.get(onelake.store(base), f"{engine}.json").bytes())
    except FileNotFoundError:
        sys.exit(f"the run succeeded but wrote no fingerprint at {base}/{engine}.json")
    fp = json.loads(body)
    out = REPO / "history" / "parity" / f"{engine}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(fp, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out}: {fp['rows_total']:,} rows, {fp['duids']} DUIDs, {fp['days']} days")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
