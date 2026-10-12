"""Pin the one destructive switch: `reset_destination` deletes the DESTINATION, never the landing.

Deleting `dbt_landing` would lose the CSVs and the archive log every engine reads -- the one
input parity is graded on -- and nothing downstream would go red: download_aemo.py would
re-land a fraction of it and every engine would agree on the smaller set.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import importlib
import sys

import pytest
import yaml

from _layout import REPO

PIPELINE = REPO / ".github" / "workflows" / "pipeline.yml"


@pytest.fixture()
def provision(monkeypatch):
    monkeypatch.setenv("FABRIC_WORKSPACE_ID", "00000000-0000-0000-0000-000000000000")
    sys.path.insert(0, str(REPO / ".github" / "scripts"))
    pytest.importorskip("requests")
    return importlib.import_module("provision")


def test_destination_is_the_data_lakehouse_and_the_catalog_only(provision):
    assert provision.DESTINATION == (("lakehouses", "dbt"), ("sqlDatabases", "dbt_ducklake_meta"))
    assert provision.LANDING_LAKEHOUSE not in {name for _, name in provision.DESTINATION}


def test_reset_deletes_only_the_destination(provision, monkeypatch):
    items = {("lakehouses", "dbt_landing"): "L", ("lakehouses", "dbt"): "D",
             ("sqlDatabases", "dbt_ducklake_meta"): "S"}
    deleted = []

    def find(kind, name):
        return items.get((kind, name))

    class R:
        status_code = 200
        text = ""

    def req(method, path, **kw):
        assert method == "DELETE"
        item = path.rsplit("/", 1)[-1]
        deleted.append(item)
        for k, v in list(items.items()):
            if v == item:
                del items[k]
        return R()

    monkeypatch.setattr(provision, "find", find)
    monkeypatch.setattr(provision, "req", req)
    provision.reset_destination()
    assert deleted == ["D", "S"]
    assert items == {("lakehouses", "dbt_landing"): "L"}


def test_reset_refuses_when_the_destination_resolves_to_the_landing(provision, monkeypatch):
    monkeypatch.setattr(provision, "find", lambda kind, name: "SAME")
    monkeypatch.setattr(provision, "req", lambda *a, **k: pytest.fail("deleted something"))
    with pytest.raises(SystemExit):
        provision.reset_destination()


def test_pipeline_resets_only_on_request_and_before_provisioning():
    wf = yaml.safe_load(PIPELINE.read_text(encoding="utf-8"))
    inp = wf[True]["workflow_dispatch"]["inputs"]["reset_destination"]
    assert inp["type"] == "boolean" and inp["default"] is False
    steps = wf["jobs"]["land"]["steps"]
    runs = [str(s.get("run", "")) for s in steps]
    reset = next(i for i, r in enumerate(runs) if "--reset-destination" in r)
    assert steps[reset]["if"] == "inputs.reset_destination"
    provision_step = next(i for i, r in enumerate(runs) if "provision.py duckrun" in r)
    assert reset < provision_step, "the reset must come before the items are provisioned"
    # Only the land job may reset: every leg waits for it, so nothing is writing meanwhile.
    for name, job in wf["jobs"].items():
        if name != "land" and "steps" in job:
            assert "--reset-destination" not in str(job["steps"]), name
