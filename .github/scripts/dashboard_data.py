#!/usr/bin/env python3
"""history/runs/*.json -> dashboard/data/runs.json, the one generated file the dashboard reads.

docs.yml runs this while it assembles the Pages site; the page's Parity and Runs tabs read
nothing else. Everything else under dashboard/data/ is hand-curated and checked in.

    python .github/scripts/dashboard_data.py [--history history/runs] [--out dashboard/data/runs.json]

Only the five engines in check_gating.py's ENGINES survive: the run records still carry the
retired Warehouse leg (`dwh`) from before it was dropped, and a sixth card would read as a
sixth engine. A record with no `run.id` (a record job that ran with nothing to record) is
skipped. Stdlib only, so tests_py/test_dashboard.py runs it in ci.yml's unit job.

The parity rule itself is NOT here: the page applies parity.py's EXACT / APPROX / REL_TOL,
which this script copies into the output as `rule`, so the page cannot drift from the
acceptance test.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_gating import ENGINES  # noqa: E402
from parity import APPROX, EXACT, REL_TOL  # noqa: E402

REPO = Path(__file__).resolve().parents[2]


def compact(record: dict) -> dict | None:
    run = record.get("run") or {}
    if not run.get("id"):
        return None
    inputs = record.get("inputs") or {}
    return {
        "run": run["id"],
        "url": run.get("url"),
        "sha": (run.get("sha") or "")[:7],
        "started": run.get("started"),
        "finished": run.get("finished"),
        "inputs": {k: inputs.get(k) for k in ("engines", "mode", "process_limit")},
        "legs": {
            e: {k: leg.get(k) for k in ("started", "finished", "outcome")}
            for e, leg in sorted((record.get("legs") or {}).items())
            if e in ENGINES
        },
        "parity": {
            e: {k: fp.get(k) for k in EXACT + APPROX}
            for e, fp in sorted((record.get("parity") or {}).items())
            if e in ENGINES
        },
    }


def build(history: Path) -> dict:
    runs = []
    for p in sorted(history.glob("*.json")):  # basename = <UTC time>-<run id>: chronological
        row = compact(json.loads(p.read_text(encoding="utf-8")))
        if row:
            runs.append(row)
    return {
        "engines": list(ENGINES),
        "rule": {"exact": EXACT, "approx": APPROX, "rel_tol": REL_TOL},
        "runs": runs,
    }


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--history", type=Path, default=REPO / "history" / "runs")
    ap.add_argument("--out", type=Path, default=REPO / "dashboard" / "data" / "runs.json")
    args = ap.parse_args(argv)
    data = build(args.history)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
    print(f"{len(data['runs'])} runs -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
