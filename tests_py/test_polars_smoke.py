"""The dbt-polars smoke test must stay a smoke test, and must stay SQL.

.github/workflows/polars_smoke.yml is the probe that made polars a SIXTH engine, and it stays
a probe: it touches no gating, no parity, no deploy, and it is dispatch-only so it costs
Fabric capacity only when somebody asks. The engine itself is `polars` in the five-engine
layout's registries; this file only pins that the PROBE stays apart from it.

They also pin the one rule of the probe itself: every model it builds is a SQL model. A
Python model would measure a dataframe rewrite, not whether the gold layer's SQL runs.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import importlib.util
import os

import pytest
import yaml

from _layout import REPO

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
    files = [{"path": f"x/Files/csv_raw/daily/{s}.CSV", "stem": s,
              "abfss": f"abfss://ws@onelake.dfs.fabric.microsoft.com/x/{s}.CSV"} for s in "BA"]
    return smoke.probes({"daily_file": files[0], "daily_files": files, "speed_files": files})


def _model(smoke, name):
    return next(p[3][name] for p in _probes(smoke) if p[3] and name in p[3])


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


def test_file_list_probe_reads_two_files(smoke):
    sql = _model(smoke, "f_read_csv_list")
    assert sql.count("read_csv(") >= 2
    assert "A.CSV" in sql and "B.CSV" in sql


def test_requirements_use_released_dbt_polars():
    """dbt-polars as released, pinned, no fork: the models read files with Polars SQL table
    functions, so nothing waits on an adapter patch."""
    reqs = [l for l in (REPO / "requirements" / "polars.txt").read_text().splitlines()
            if l.strip() and not l.startswith("#")]
    assert not any("incubator-dbt-polars" in l or "git+" in l for l in reqs)
    assert any(l.startswith("dbt-polars[iceberg]==") for l in reqs)


def test_only_the_source_probe_gets_the_file_catalog(smoke):
    """The source probe needs a storage catalog (dbt-polars-catalog-azure) for its
    external.location; every other probe runs on the Iceberg catalog alone, so a broken azure
    catalog cannot fail the rest."""
    def types(landing):
        cats = smoke.profile_yml(landing)["polars_smoke"]["outputs"]["smoke"]["catalogs"]
        return [c["type"] for c in cats]
    assert types(False) == ["iceberg"]
    assert types(True) == ["iceberg", "azure"]
    with_yml = [p for p in _probes(smoke) if any(m.endswith(".yml") for m in (p[3] or {}))]
    assert [m for p in with_yml for m in p[3]] == ["sources.yml", "f_source_glob"]


def test_the_source_reads_with_scan_csv_options(smoke):
    """read_options are pl.scan_csv's parameters, and missing_columns is the one Polars SQL
    refuses (pola-rs/polars#29970): it is the reason for this route."""
    table = smoke.SOURCES_YML["sources"][0]["tables"][0]
    assert table["external"]["location"].endswith("/*.CSV")
    opts = table["config"]["read_options"]
    assert opts["missing_columns"] == "insert" and len(opts["schema"]) == 131
    assert "source('landed', 'daily')" in _model(smoke, "f_source_glob")


def test_the_smoke_installs_the_engine_pin():
    smoke = (REPO / "requirements" / "polars_smoke.txt").read_text()
    assert "-r polars.txt" in smoke


def test_picker_probe_uses_neither_hook_nor_merge_clauses(smoke):
    """It stands in for both, so using either would answer a different question."""
    sql = _model(smoke, "m_picker")
    assert "pre_hook" not in sql and "merge_clauses" not in sql
    assert "run_query(" in sql and "this" in sql
