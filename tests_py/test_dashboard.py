"""The dashboard (dashboard/, published at the Pages root) is hand-curated JSON read by static
HTML, so nothing at publish time notices it drifting from the repo: an engine added or dropped,
a cited file renamed, a bug without a link. These pin it, plus the one generated file.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import json
import re
import sys

from _layout import ENGINES, REPO

sys.path.insert(0, str(REPO / ".github" / "scripts"))
import dashboard_data  # noqa: E402

DASH = REPO / "dashboard"
DATA = DASH / "data"
CURATED = ["capability", "syntax", "bugs"]
ISSUE = re.compile(r"^https://github\.com/[\w.-]+/[\w.-]+/(issues|pull)/\d+$")


def data(name):
    return json.loads((DATA / f"{name}.json").read_text(encoding="utf-8"))


def test_the_curated_files_exist_and_parse():
    for name in CURATED:
        assert isinstance(data(name), dict), name


def test_the_engine_columns_are_the_five_engines():
    """An engine added to (or dropped from) the project must show up on the page -- the
    matrix and the snippets are the two views that would otherwise silently omit it."""
    page = re.findall(r"^  (\w+): +\{label:", (DASH / "index.html").read_text(encoding="utf-8"), re.M)
    assert sorted(page) == sorted(ENGINES), page
    assert page[0] == "spark", "spark is the reference implementation and the parity baseline"
    assert data("capability")["engines"] == page
    assert data("syntax")["engines"] == page
    assert dashboard_data.ENGINES.keys() == set(ENGINES)


def test_every_capability_row_has_every_engine():
    cap = data("capability")
    for row in cap["rows"]:
        assert row["group"] in cap["groups"], row["label"]
        assert set(row["cells"]) == set(ENGINES), f"{row['label']}: {sorted(row['cells'])}"
        for engine, cell in row["cells"].items():
            assert cell["o"] in {"native", "workaround", "no"}, (row["label"], engine)
            assert cell.get("n"), f"{row['label']} / {engine} has no note: no cell without a source"


def test_every_syntax_concept_covers_every_engine_once():
    for c in data("syntax")["concepts"]:
        seen = [e for p in c["panels"] for e in p["engines"]]
        assert sorted(seen) == sorted(ENGINES), f"{c['id']}: {seen}"


def test_every_cited_path_exists():
    """A file renamed or moved would leave a dead link on the page."""
    paths = [cell["path"] for row in data("capability")["rows"]
             for cell in row["cells"].values() if cell.get("path")]
    paths += [p["path"] for c in data("syntax")["concepts"] for p in c["panels"]]
    bugs = data("bugs")
    paths += [u["source"] for u in bugs["unfiled"]] + [t["details"] for t in bugs["tried"]]
    missing = sorted({p for p in paths if not (REPO / p).exists()})
    assert not missing, missing


def test_every_bug_links_its_upstream_issue():
    bugs = data("bugs")
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", bugs["as_of"])
    for project in bugs["projects"]:
        assert set(project["engines"]) <= set(ENGINES), project["name"]
        assert project["items"], project["name"]
        for item in project["items"]:
            assert ISSUE.match(item["url"]), item["url"]
            assert item["status"] and item["text"], item["url"]
    for t in bugs["tried"]:
        assert ISSUE.match(t["issue"]["url"]), t["name"]


def test_the_page_loads_only_its_own_files():
    """No CDN and no absolute data URL: the page and its data publish together."""
    html = (DASH / "index.html").read_text(encoding="utf-8")
    assert "<script src" not in html and "<link rel=\"stylesheet\"" not in html
    assert "fetch(`data/${name}.json`)" in html
    assert 'href="dbt/"' in html


def _record(run_id, legs, parity):
    return {
        "run": {"id": run_id, "url": f"https://github.com/x/y/actions/runs/{run_id}",
                "sha": "abcdef0123", "started": "2026-10-01T00:00:00Z",
                "finished": "2026-10-01T00:20:00Z"},
        "inputs": {"engines": "all", "mode": "local", "process_limit": "50", "dbt_schema": "mart"},
        "legs": {e: {"started": "2026-10-01T00:05:00Z", "finished": "2026-10-01T00:10:00Z",
                     "outcome": "success"} for e in legs},
        "parity": {e: {"engine": e, "rows_total": 10, "duids": 2, "days": 1,
                       "date_min": "2026-09-30", "date_max": "2026-09-30",
                       "mw_sum": 1.5, "price_sum": 2.5} for e in parity},
        "layout": {"big": "dropped"},
    }


def test_dashboard_data_compacts_the_history(tmp_path):
    hist = tmp_path / "runs"
    hist.mkdir()
    (hist / "2026-10-01T0020Z-2.json").write_text(json.dumps(_record("2", ["duckrun", "dwh"], ["duckrun", "dwh"])))
    (hist / "2026-09-30T0020Z-1.json").write_text(json.dumps(_record("1", ["spark"], ["spark"])))
    (hist / "2026-10-02T0000Z-x.json").write_text(json.dumps({"run": {"finished": "2026-10-02T00:00:00Z"}}))
    out = dashboard_data.build(hist)

    assert [r["run"] for r in out["runs"]] == ["1", "2"], "chronological, and a record with no run id is skipped"
    latest = out["runs"][-1]
    assert set(latest["legs"]) == set(latest["parity"]) == {"duckrun"}, "the retired dwh leg is dropped"
    assert set(latest["parity"]["duckrun"]) == set(out["rule"]["exact"] + out["rule"]["approx"])
    assert "layout" not in latest and latest["sha"] == "abcdef0"
    assert out["rule"]["rel_tol"] == dashboard_data.REL_TOL
