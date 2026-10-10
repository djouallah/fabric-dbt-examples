"""The dbt-polars smoke test must stay a smoke test, and must stay SQL.

.github/workflows/polars_smoke.yml probes whether dbt-polars could become a SIXTH engine. It
is not one: it touches no gating, no parity, no deploy, and it is dispatch-only so it costs
Fabric capacity only when somebody asks. These tests pin that absence, the way
test_sail_smoke.py does for Sail.

They also pin the one rule of the probe itself: every model it builds is a SQL model. A
Python model would measure a dataframe rewrite, not whether the gold layer's SQL runs.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import importlib.util
import os

import pytest
import yaml

from _layout import ENGINES, REPO

SMOKE_PY = REPO / ".github" / "scripts" / "polars_smoke.py"
SMOKE_YML = REPO / ".github" / "workflows" / "polars_smoke.yml"


@pytest.fixture(scope="module")
def smoke():
    """Import polars_smoke.py with dummy credentials. Nothing here connects to anything."""
    os.environ.setdefault("WAREHOUSE_PATH", "ws-guid/item-guid")
    os.environ.setdefault("ONELAKE_TOKEN", "dummy-token")
    os.environ.setdefault("LANDING_ITEM", "landing-guid")
    spec = importlib.util.spec_from_file_location("polars_smoke", SMOKE_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def workflow():
    return yaml.safe_load(SMOKE_YML.read_text(encoding="utf-8"))


def _probes(smoke):
    ctx = {"daily_file": {"path": "x/Files/csv_raw/daily/A.CSV", "stem": "A",
                          "abfss": "abfss://ws@onelake.dfs.fabric.microsoft.com/x/A.CSV"}}
    return smoke.probes(ctx)


def test_polars_is_not_a_registered_engine():
    assert "polars" not in ENGINES


def test_there_is_no_engine_shaped_requirements_file():
    assert not (REPO / "requirements" / "polars.txt").exists()


def test_there_is_no_polars_model_tree():
    assert not (REPO / "dbt1" / "models" / "aemo" / "polars").exists()


def test_workflow_is_dispatch_only(workflow):
    # PyYAML reads the bare `on:` key as True.
    triggers = workflow.get("on", workflow.get(True))
    assert set(triggers) == {"workflow_dispatch"}


def test_workflow_never_gates(workflow):
    assert workflow["jobs"]["smoke"].get("continue-on-error") is True


def test_no_other_workflow_calls_it():
    for wf in (REPO / ".github" / "workflows").glob("*.yml"):
        if wf.name != SMOKE_YML.name:
            assert "polars_smoke" not in wf.read_text(encoding="utf-8"), wf.name


def test_script_always_exits_zero():
    src = SMOKE_PY.read_text(encoding="utf-8")
    assert "os._exit(0)" in src


def test_every_model_is_sql(smoke):
    """No Python model, anywhere: the question is whether the SQL runs."""
    for number, name, _, models, _, _ in _probes(smoke):
        for model, sql in (models or {}).items():
            assert "def model(" not in sql, f"probe {number} ({name}) builds a Python model"
            assert "dbt.ref(" not in sql and "session" not in sql, (number, model)


def test_probe_numbers_are_contiguous(smoke):
    numbers = [p[0] for p in _probes(smoke)]
    assert numbers == list(range(1, len(numbers) + 1))


def test_model_names_are_unique(smoke):
    names = [m for p in _probes(smoke) for m in (p[3] or {})]
    assert len(names) == len(set(names))


@pytest.mark.parametrize("schema", ["dbo", "iceberg_mart", "duckrun_landing"])
def test_it_refuses_a_real_legs_namespace(schema, monkeypatch):
    monkeypatch.setenv("POLARS_SMOKE_SCHEMA", schema)
    spec = importlib.util.spec_from_file_location("polars_smoke_guard", SMOKE_PY)
    mod = importlib.util.module_from_spec(spec)
    with pytest.raises(SystemExit):
        spec.loader.exec_module(mod)


def test_the_token_never_lands_in_the_profile(smoke):
    profile = str(smoke.profile_yml())
    assert "dummy-token" not in profile
    assert "DBT_ENV_SECRET_" in profile


def test_merge_probe_needs_a_real_merge(smoke):
    """Key 1 is only in batch 1: a rebuild instead of a merge loses it, so it must be expected."""
    assert (1, 1, 1, 10) in smoke.MERGE_EXPECTED
    assert (2, 2, 2, 99) in smoke.MERGE_EXPECTED


def test_insert_only_probe_asks_for_do_nothing(smoke):
    assert "do_nothing" in smoke.INSERT_ONLY_SQL
    assert (1, 10) in smoke.INSERT_ONLY_EXPECTED
