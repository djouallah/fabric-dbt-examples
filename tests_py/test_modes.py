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


def test_every_engine_builds_through_run_dbt():
    """One build step, for every engine, with no condition: an engine run_dbt.py does not
    know dies on its usage line instead of quietly building nothing."""
    src = (REPO / ".github" / "scripts" / "run_dbt.py").read_text(encoding="utf-8")
    m = re.search(r"^PROJECT = \{(.*?)\}", src, re.M | re.S)
    assert m, "run_dbt.py has no PROJECT dict"
    assert set(re.findall(r'"([a-z]+)":', m.group(1))) == set(ENGINES)
    steps = [s for s in build()["steps"] if "run_dbt.py" in str(s.get("run", ""))]
    assert len(steps) == 1 and "if" not in steps[0]


def test_every_leg_fingerprints_in_process():
    """A leg that writes no history/parity/<engine>.json is INVISIBLE: `parity.py compare` with
    fewer than two fingerprints returns 0. And it must be IN-PROCESS: spark's session (jars,
    OneLake auth, registered tables) exists only inside run_dbt.py."""
    steps = build()["steps"]
    run = [s for s in steps if "run_dbt.py" in str(s.get("run", ""))][0]["run"]
    assert '--fingerprint-out "history/parity/$ENGINE.json"' in run
    assert "--no-fingerprint" not in run
    assert not [s for s in steps if "run-operation parity_fingerprint" in str(s.get("run", ""))], (
        "a separate `dbt run-operation` step would start a fresh process -- for spark, a JVM "
        "with no session setup"
    )


def test_the_runner_transport_is_set_for_every_leg():
    """DuckDB fails the OneLake TLS handshake on the runner without these."""
    for name in ("AZURE_TRANSPORT_OPTION_TYPE", "CURL_CA_INFO", "DUCKDB_TEMP_DIR"):
        assert name in build()["env"], f"{name} is no longer set at job level"


def test_local_runs_default_to_a_small_process_limit():
    """Every in-process engine folds its batch in the runner's 7 GB."""
    default = workflow()[True]["workflow_dispatch"]["inputs"]["process_limit"]["default"]
    assert int(default) <= 100, f"process_limit defaults to {default}"
