"""The spark engine's SparkSession, built BEFORE dbt starts: local PySpark on the runner,
Delta through delta-spark, OneLake through hadoop-azure. dbt-spark's `method: session` then
picks this session up with getOrCreate() -- which is the only way to give it jars, Delta's
catalog and OneLake auth, because none of those can be set once a JVM is running.

Three things it does that dbt-spark cannot:

  1. ONELAKE AUTH WITH NO SECRET. Hadoop's ABFS driver has no Azure CLI credential, so it
     authenticates as the GitHub job's federated identity: WorkloadIdentityTokenProvider
     exchanges the job's OIDC token -- written to a file, and re-written every few minutes by a
     daemon thread, because a GitHub OIDC token lives minutes -- for an Entra token. The same
     app registration and federated credential azure/login uses; no client secret anywhere.
     Off GitHub, point AZURE_FEDERATED_TOKEN_FILE at a token file yourself.
  2. THE TWO DATABASES, AT THEIR ONELAKE LOCATION. spark_landing / spark_mart (or
     <DBT_SCHEMA>_spark_*) are created with LOCATION <dbt lakehouse>/Tables/<schema>, so a
     managed table lands exactly where every other engine's schema folder is.
  3. RE-REGISTER WHAT IS ALREADY THERE. A fresh runner's metastore is EMPTY, so dbt-spark would
     see no relations, `is_incremental()` would be false on every run, and every incremental
     model would rebuild from scratch. Each Delta table already under those folders is
     registered first.
"""
from __future__ import annotations

import glob
import json
import os
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"
TOKEN_REFRESH_SECONDS = 240


def _github_oidc_token() -> str:
    url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"] + "&audience=api://AzureADTokenExchange"
    req = urllib.request.Request(url, headers={
        "Authorization": f"bearer {os.environ['ACTIONS_ID_TOKEN_REQUEST_TOKEN']}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)["value"]


def federated_token_file() -> str:
    """A file holding a fresh federated token. On GitHub Actions a daemon thread keeps it
    fresh; elsewhere AZURE_FEDERATED_TOKEN_FILE must already point at one."""
    if os.environ.get("AZURE_FEDERATED_TOKEN_FILE"):
        return os.environ["AZURE_FEDERATED_TOKEN_FILE"]
    if not os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL"):
        raise SystemExit("spark needs a federated token: run on GitHub Actions with "
                         "`id-token: write`, or set AZURE_FEDERATED_TOKEN_FILE")
    path = os.path.join(tempfile.mkdtemp(), "federated-token")

    def write() -> None:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(_github_oidc_token())
        os.replace(tmp, path)

    write()

    def refresh() -> None:
        while True:
            time.sleep(TOKEN_REFRESH_SECONDS)
            try:
                write()
            except Exception as e:  # noqa: BLE001 -- the next tick tries again
                print(f"  ! federated token refresh failed: {e}", flush=True)

    threading.Thread(target=refresh, daemon=True).start()
    return path


def _hadoop_version() -> str:
    """The Hadoop pyspark bundles -- hadoop-azure must match it exactly."""
    import pyspark

    jars = glob.glob(os.path.join(os.path.dirname(pyspark.__file__), "jars",
                                  "hadoop-client-api-*.jar"))
    if not jars:
        raise SystemExit("no hadoop-client-api jar in pyspark; cannot pick hadoop-azure")
    return Path(jars[0]).stem.rsplit("-", 1)[-1]


def schemas() -> list[str]:
    """spark_landing / spark_mart under generate_schema_name()'s rule (deploy.mart_schema)."""
    sys.path.insert(0, str(SCRIPTS.parents[1]))
    from deploy import mart_schema

    mart = mart_schema("spark")
    return [mart[: -len("_mart")] + "_landing", mart]


def start() -> None:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    tables = os.environ["SPARK_TABLES_PATH"].rstrip("/")
    token_file = federated_token_file()
    abfs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type":
            "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
        "fs.azure.account.oauth2.msi.tenant": os.environ["AZURE_TENANT_ID"],
        "fs.azure.account.oauth2.client.id": os.environ["AZURE_CLIENT_ID"],
        "fs.azure.account.oauth2.token.file": token_file,
    }
    warehouse = os.path.join(tempfile.mkdtemp(), "spark-warehouse")
    builder = (SparkSession.builder.master("local[*]").appName("dbt-aemo")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
               .config("spark.sql.warehouse.dir", warehouse)
               .config("spark.local.dir", os.environ.get("SPARK_LOCAL_DIR", "/tmp/spark_local"))
               # A dropped `<model>__stage` can leave its folder behind for a moment; a CTAS
               # into the same managed location must not refuse it.
               .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true"))
    for k, v in abfs.items():
        builder = builder.config(f"spark.hadoop.{k}", v)
    hadoop = _hadoop_version()
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=[f"org.apache.hadoop:hadoop-azure:{hadoop}"]).getOrCreate()
    print(f"spark {spark.version}, hadoop {hadoop}, tables at {tables}", flush=True)

    import onelake

    for schema in schemas():
        location = f"{tables}/{schema}"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema} LOCATION '{location}'")
        registered = 0
        for name in onelake.list_dirs(location):
            if "_delta_log" not in onelake.list_dirs(f"{location}/{name}"):
                continue
            spark.sql(f"CREATE TABLE IF NOT EXISTS {schema}.`{name}` USING DELTA "
                      f"LOCATION '{location}/{name}'")
            registered += 1
        print(f"  {schema}: {registered} existing Delta tables registered", flush=True)
