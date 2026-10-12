"""Pin the two ways this repo runs, and the partition of engines across build steps.

TWO MODES ONLY. `pipeline.yml` builds every engine ON THE RUNNER; running in Fabric means
deploying the items and running the deployed pipeline (deploy.py). There is no third way --
the throwaway-notebook launcher (`remote_dbt.py`, duckrun's run_python) is gone, and duckrun is
only the dbt adapter.

EVERY WAY THE BUILD PARTITION BREAKS IS GREEN: an engine that reaches no build step is simply
skipped, and the fingerprint step then publishes the previous run's numbers.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re

import yaml

from _layout import ENGINES, REPO

PIPELINE = REPO / ".github" / "workflows" / "pipeline.yml"
WORKFLOWS = REPO / ".github" / "workflows"


def workflow() -> dict:
    return yaml.safe_load(PIPELINE.read_text(encoding="utf-8"))


def build() -> dict:
    return workflow()["jobs"]["build"]


def steps_matching(needle: str) -> list[dict]:
    return [s for s in build()["steps"] if needle in str(s.get("run", ""))]


def test_no_remote_execution_anywhere():
    """The third mode is gone, and nothing may quietly bring it back."""
    assert not (REPO / ".github" / "scripts" / "remote_dbt.py").exists()
    for wf in WORKFLOWS.glob("*.yml"):
        src = wf.read_text(encoding="utf-8")
        for needle in ("remote_dbt", "run_python", "local_runner", "FABRIC_CORES", "fabric_cores"):
            assert needle not in src, f"{wf.name} still mentions {needle}"


def test_every_leg_logs_in():
    """duckrun no longer mints the scripts' tokens from the OIDC assertion, so no leg is exempt."""
    login = [s for s in build()["steps"] if "azure/login" in str(s.get("uses", ""))]
    assert len(login) == 1
    assert "if" not in login[0], f"the build's azure/login is conditional: {login[0].get('if')!r}"


def test_a_leg_mints_the_tokens_its_profile_needs():
    """The two fail DIFFERENTLY: env_var('ONELAKE_TOKEN') has no default, so dbt dies at
    profile render, while DBT_ENV_SECRET_SQL_TOKEN defaults to '' -- the profile renders and the
    leg dies later at the DuckLake ATTACH, on a login error that reads like a catalog fault."""
    onelake = steps_matching("ONELAKE_TOKEN=")
    assert onelake, "no build step mints ONELAKE_TOKEN"
    cond = str(onelake[0].get("if", ""))
    for engine in ("iceberg", "ducklake", "polars"):
        assert engine in cond, f"{engine} reads env_var('ONELAKE_TOKEN') but is not in {cond!r}"
    assert "add-mask" in str(onelake[0]["run"]), "the minted token is not masked in the log"

    sql = steps_matching("DBT_ENV_SECRET_SQL_TOKEN=")
    assert sql and "ducklake" in str(sql[0].get("if", ""))
    run = str(sql[0]["run"])
    assert "https://database.windows.net/" in run and "add-mask" in run


def test_every_engine_reaches_exactly_one_build_step():
    src = (REPO / ".github" / "scripts" / "run_dbt.py").read_text(encoding="utf-8")
    m = re.search(r"^PROJECT = \{(.*?)\}", src, re.M | re.S)
    assert m, "run_dbt.py has no PROJECT dict"
    driver = set(re.findall(r'"([a-z]+)":', m.group(1)))

    client_step = [s for s in build()["steps"]
                   if s.get("name") == "dbt build" and "working-directory" in s]
    assert len(client_step) == 1, "the dwh/spark build step is not where this test expects it"
    cond = str(client_step[0]["if"])
    client = {e for e in ENGINES if f"matrix.engine == '{e}'" in cond}

    assert driver & client == set(), f"{driver & client} matches two build steps"
    assert driver | client == set(ENGINES), (
        f"{set(ENGINES) - (driver | client)} reaches no build step at all"
    )
    driver_step = [s for s in build()["steps"] if s.get("name") == "dbt build on this runner"]
    assert len(driver_step) == 1
    for engine in client:
        assert f"matrix.engine != '{engine}'" in str(driver_step[0]["if"])


def test_every_leg_fingerprints_exactly_once():
    """A leg that writes no history/parity/<engine>.json is INVISIBLE: `parity.py compare` with
    fewer than two fingerprints returns 0. The acceptance test silently does not run."""
    steps = build()["steps"]
    driver = [s for s in steps if s.get("name") == "dbt build on this runner"]
    assert driver and "--no-fingerprint" in str(driver[0]["run"])
    fp = [s for s in steps if s.get("name") == "Fingerprint gold layer"]
    assert len(fp) == 1 and "if" not in fp[0], (
        "the fingerprint step is conditional -- a carve-out leaves a leg with no fingerprint"
    )


def test_the_runner_transport_is_set_for_every_leg():
    """DuckDB fails the OneLake TLS handshake on the runner without these."""
    for name in ("AZURE_TRANSPORT_OPTION_TYPE", "CURL_CA_INFO", "DUCKDB_TEMP_DIR"):
        assert name in build()["env"], f"{name} is no longer set at job level"


def test_local_runs_default_to_a_small_process_limit():
    """Every in-process engine folds its batch in the runner's 7 GB."""
    default = workflow()[True]["workflow_dispatch"]["inputs"]["process_limit"]["default"]
    assert int(default) <= 100, f"process_limit defaults to {default}"
