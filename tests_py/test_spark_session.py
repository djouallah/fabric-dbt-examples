"""Pin the spark leg's two switches: where its gold layer goes (SPARK_OUTPUT) and Gluten.

EVERY WAY THESE BREAK IS GREEN OR SLOW, NOT RED:
  * a local output that still honours process_limit folds the newest N files into an empty
    warehouse every run -- a clean build, every test a warning, and parity never reached;
  * layout.py / deploy.py pointed at spark in the local output read or bind the lakehouse's
    spark_mart, which is an older generation, under this run's id;
  * a Gluten bundle whose Spark or Delta does not match pyspark's dies at session start at best;
  * a native abfss:// scan passes Gluten's validation and fails inside Velox's ABFS client.

Offline: imports spark_session without pyspark (its heavy imports are inside start()).

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re
import sys

import pytest
import yaml

from _layout import REPO

sys.path.insert(0, str(REPO / ".github" / "scripts"))
import spark_session  # noqa: E402

PIPELINE = REPO / ".github" / "workflows" / "pipeline.yml"


def test_output_defaults_to_local_and_refuses_anything_else(monkeypatch):
    monkeypatch.delenv("SPARK_OUTPUT", raising=False)
    assert spark_session.output() == "local"
    monkeypatch.setenv("SPARK_OUTPUT", "")
    assert spark_session.output() == "local"
    monkeypatch.setenv("SPARK_OUTPUT", "OneLake")
    assert spark_session.output() == "onelake"
    monkeypatch.setenv("SPARK_OUTPUT", "lakehouse")
    with pytest.raises(SystemExit):
        spark_session.output()


def test_the_local_output_starts_empty(tmp_path, monkeypatch):
    """Lifting process_limit is only right against an EMPTY warehouse."""
    target = tmp_path / "tables"
    (target / "spark_mart" / "fct_summary").mkdir(parents=True)
    monkeypatch.setenv("SPARK_LOCAL_TABLES", str(target))
    uri = spark_session.local_tables()
    assert uri.startswith("file:") and not uri.endswith("/")
    assert target.is_dir() and not any(target.iterdir())


def test_the_local_output_lifts_process_limit():
    src = (REPO / ".github" / "scripts" / "spark_session.py").read_text(encoding="utf-8")
    start = src[src.index("def start()"):]
    local = start[start.index('if out == "local":'):start.index("else:")]
    assert 'os.environ["process_limit"] = UNLIMITED' in local
    assert int(spark_session.UNLIMITED) >= 100_000
    # ...and only there: the onelake output is incremental and keeps the knob.
    assert start.count("process_limit\"] =") == 1


def test_gluten_keeps_scans_in_the_jvm():
    conf = spark_session.gluten_conf("/x/gluten.jar")
    assert conf["spark.plugins"] == "org.apache.gluten.GlutenPlugin"
    assert conf["spark.driver.extraClassPath"] == "/x/gluten.jar"
    assert conf["spark.memory.offHeap.enabled"] == "true"
    assert conf["spark.gluten.sql.columnar.filescan"] == "false"
    assert conf["spark.gluten.sql.columnar.batchscan"] == "false"


def test_ansi_is_off_whether_or_not_gluten_is():
    """Gluten falls back under ANSI; and an accelerator toggle must not change the numbers, so
    the setting sits on the builder, not in gluten_conf()."""
    src = (REPO / ".github" / "scripts" / "spark_session.py").read_text(encoding="utf-8")
    assert '.config("spark.sql.ansi.enabled", "false")' in src
    assert "ansi" not in str(spark_session.gluten_conf("j"))


def test_pyspark_and_delta_match_the_gluten_bundle():
    """Gluten 1.7.0's spark-4.0 profile: Spark 4.0.2, Delta 4.0.1."""
    assert spark_session.GLUTEN_VERSION == "1.7.0"
    assert "spark-4.0" in spark_session.GLUTEN_TARBALL
    assert "spark4.0_2.13" in spark_session.GLUTEN_JAR
    assert re.fullmatch(r"[0-9a-f]{128}", spark_session.GLUTEN_SHA512)
    reqs = (REPO / "requirements" / "spark.txt").read_text(encoding="utf-8")
    assert re.search(r"^pyspark==4\.0\.2$", reqs, re.M)
    assert re.search(r"^delta-spark==4\.0\.1$", reqs, re.M)


def test_pipeline_wires_spark_output():
    wf = yaml.safe_load(PIPELINE.read_text(encoding="utf-8"))
    inp = wf[True]["workflow_dispatch"]["inputs"]["spark_output"]
    assert inp["default"] == "local" and set(inp["options"]) == set(spark_session.OUTPUTS)
    jobs = wf["jobs"]
    assert jobs["build"]["env"]["SPARK_OUTPUT"] == "${{ inputs.spark_output }}"
    assert "onelake" in jobs["plan"]["outputs"]
    # What reads or binds the LAKEHOUSE follows the engines that rewrote it, never `engines`.
    assert "needs.plan.outputs.onelake" in jobs["layout"]["env"]["BUILD_ENGINES"]
    assert "needs.plan.outputs.onelake != '[]'" in jobs["layout"]["if"]
    assert "needs.plan.outputs.onelake" in jobs["deploy"]["env"]["DEPLOY_ENGINES"]
    pick = jobs["plan"]["steps"][0]["run"]
    assert 'select(.engine!="spark")' in pick[pick.index("spark_output"):]
