#!/usr/bin/env python3
"""Compare the gold-layer fingerprints the five engines emitted. The acceptance test.

Five adapters, one set of business logic, one gold table. This is what makes that a
checkable claim rather than an assertion in a README.

Each engine's leg fingerprints through its OWN adapter, in-process, as the last step of
run_dbt.py -- `dbt run-operation parity_fingerprint`, its log parsed by extract() below:

    python .github/scripts/run_dbt.py <engine> --fingerprint-out history/parity/<engine>.json

(`capture` does the same for a run-operation log piped in by hand), and once the legs are
done:

    python .github/scripts/parity.py compare history/parity

Exact-match fields (rows_total, duids, days, date_min, date_max) must be IDENTICAL: a
difference there is a real difference in what the pipeline produced. The money columns get
a relative tolerance, because two engines can compute the same thing and still disagree in
the last place:

  * DOUBLE -> DECIMAL tie-breaking is HALF_UP on Spark and HALF_EVEN on DuckDB. A neutral
    reader cannot grade a writer's rounding.
  * Summing ~10^5 doubles in a different order gives a different last bit.

The tolerance is 1e-7 relative. It is there to absorb rounding and float association, not
to paper over a logic difference -- if a leg drifts past it, that is a finding, not a
threshold to raise. Measured 2026-09-16: ducklake and the since-dropped Warehouse leg (dwh,
T-SQL, a third tie-breaking rule) with IDENTICAL row counts (26.8M rows, a clean
recomputation on both) differed by 1.0e-8 on mw_sum and 3.7e-8 on price_sum -- 26.8M
DECIMAL(18,4) roundings. The old 1e-9 failed that pair.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

EXACT = ["rows_total", "duids", "days", "date_min", "date_max"]
APPROX = ["mw_sum", "price_sum"]
REL_TOL = 1e-7


def extract(text: str) -> dict | None:
    """The fingerprint object in a `dbt run-operation parity_fingerprint` log, or None.

    The JSON comes out of a log() call, and dbt 1.x and 2.x do not prefix log lines the
    same way -- v2 can put its own text on the line carrying the closing brace. So the
    terminator is "a newline, then a closing brace with nothing structural before it",
    not "a brace alone at the start of a line". Both shapes are covered by
    tests_py/test_parity_capture.py."""
    m = re.search(r"\{\s*\"engine\".*?\n[^\n{}]*\}", text, re.S)
    return json.loads(m.group(0)) if m else None


def capture(out_dir: Path) -> int:
    """Read a `dbt run-operation parity_fingerprint` log on stdin, save the JSON object."""
    text = sys.stdin.read()
    fp = extract(text)
    if fp is None:
        print("no fingerprint JSON found in the run-operation output", file=sys.stderr)
        print(text[-2000:], file=sys.stderr)
        return 1
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{fp['engine']}.json"
    path.write_text(json.dumps(fp, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {path}: {fp['rows_total']:,} rows, {fp['duids']} DUIDs, {fp['days']} days")
    return 0


def compare(in_dir: Path) -> int:
    fps = {}
    for p in sorted(in_dir.glob("*.json")):
        fp = json.loads(p.read_text(encoding="utf-8"))
        fps[fp["engine"]] = fp

    if len(fps) < 2:
        print(f"only {len(fps)} fingerprint(s) in {in_dir}; need at least 2 to compare")
        return 0

    engines = sorted(fps)
    print(f"comparing {len(engines)} engines: {', '.join(engines)}\n")
    header = "field".ljust(14) + "".join(e.rjust(20) for e in engines)
    print(header)
    print("-" * len(header))
    for field in EXACT + APPROX:
        row = field.ljust(14)
        for e in engines:
            v = fps[e].get(field)
            row += (f"{v:,}" if isinstance(v, int) else f"{v:.6f}" if isinstance(v, float) else str(v)).rjust(20)
        print(row)
    print()

    base_name = engines[0]
    base = fps[base_name]
    failures: list[str] = []

    for e in engines[1:]:
        fp = fps[e]
        for field in EXACT:
            if fp.get(field) != base.get(field):
                failures.append(
                    f"{field}: {base_name}={base.get(field)!r} but {e}={fp.get(field)!r}"
                )
        for field in APPROX:
            a, b = float(base.get(field, 0)), float(fp.get(field, 0))
            scale = max(abs(a), abs(b), 1.0)
            if abs(a - b) / scale > REL_TOL:
                failures.append(
                    f"{field}: {base_name}={a!r} but {e}={b!r} "
                    f"(relative difference {abs(a - b) / scale:.3e} > {REL_TOL:.0e})"
                )

    if failures:
        print("PARITY FAILED — the engines did not produce the same gold layer:", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        print(
            "\nBefore changing a tolerance, check the two usual causes:\n"
            "  * a string join key with a trailing space (it once split the since-dropped\n"
            "    T-SQL leg, which pads on comparison), which changes row counts, not just sums\n"
            "  * one engine's model drifting from the shared business logic — diff the\n"
            "    dbt1/models/aemo/<engine>/ copies against each other.\n"
            "    `diff -r dbt1/models/aemo/duckrun dbt1/models/aemo/iceberg` is the\n"
            "    comparison that catches a drift inside the DuckDB family.",
            file=sys.stderr,
        )
        return 1

    print(f"PARITY OK — {len(engines)} engines agree on {base['model']} "
          f"({base['rows_total']:,} rows, {base['days']} days)")
    return 0


def main(argv: list[str]) -> int:
    if len(argv) < 2 or argv[1] not in ("capture", "compare"):
        print(__doc__)
        return 2
    d = Path(argv[2]) if len(argv) > 2 else Path("history/parity")
    return capture(d) if argv[1] == "capture" else compare(d)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
