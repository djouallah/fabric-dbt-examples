"""The spark engine's SparkSession, built BEFORE dbt starts: local PySpark on the runner,
Delta through delta-spark, OneLake through hadoop-azure, operators on Velox through Apache
Gluten. dbt-spark's `method: session` then picks this session up with getOrCreate() -- which is
the only way to give it jars, Delta's catalog, a plugin and OneLake auth, because none of those
can be set once a JVM is running.

WHERE THE GOLD LAYER GOES: SPARK_OUTPUT.
  local    (the default) a fresh directory on the runner's disk. The landing zone is still
           READ from the dbt_landing lakehouse -- the same files every engine reads -- but
           nothing is written to OneLake. The directory starts EMPTY every run, so every model is
           a first build and process_limit is LIFTED: the whole backlog is folded in one run.
           That is the point. Honouring process_limit against an empty warehouse would fold the
           newest N files and nothing else, every run, and the leg could never reach parity with
           engines that converged over many runs. The fingerprint is taken in this process,
           before the directory goes. The cost: spark_mart in the lakehouse is not refreshed, so
           its Direct Lake model and layout.py's read-back describe the last `onelake` run
           (pipeline.yml leaves spark out of both in this output).
  onelake  spark_landing / spark_mart in the shared `dbt` lakehouse over abfss://, incremental
           across runs, bounded by process_limit like every other leg -- the behaviour before
           SPARK_OUTPUT existed.

GLUTEN + VELOX: SPARK_GLUTEN (default 1). The official Apache Gluten release bundle for Spark
4.0, which is why pyspark / delta-spark are pinned to the versions it is built against
(requirements/spark.txt). What it does and does not offload here:
  * File SCANS STAY IN THE JVM (spark.gluten.sql.columnar.filescan=false). The release is built
    with ABFS, so an abfss:// scan passes Gluten's file-system validation and is handed to
    Velox's own ABFS client, which knows neither the OneLake host nor the workload-identity
    token file below -- the read would fail at run time rather than fall back. The ragged AEMO
    CSVs are no candidate for a native text reader either.
  * NO DELTA COMPONENT. The release bundle carries Iceberg and Paimon, not Delta (that needs a
    from-source build with -Pdelta), so Delta scans and writes run in the JVM as before.
  * Velox runs what sits between them -- filter, project (the casts), aggregate, join, sort,
    window, shuffle -- behind a row-to-columnar transition.
  * ANSI mode is OFF. Gluten requires it (it falls back to vanilla Spark under ANSI), and it is
    the Fabric Spark 3.5 semantics these models were written, and reached parity, against. Off
    with SPARK_GLUTEN=0 too: an accelerator toggle must not change the numbers.

Three things it does that dbt-spark cannot:

  1. ONELAKE AUTH WITH NO SECRET. Hadoop's ABFS driver has no Azure CLI credential, so it
     authenticates as the GitHub job's federated identity: WorkloadIdentityTokenProvider
     exchanges the job's OIDC token -- written to a file, and re-written every few minutes by a
     daemon thread, because a GitHub OIDC token lives minutes -- for an Entra token. The same
     app registration and federated credential azure/login uses; no client secret anywhere.
     Off GitHub, point AZURE_FEDERATED_TOKEN_FILE at a token file yourself. Needed in BOTH
     outputs: the landing zone is OneLake either way.
  2. THE TWO DATABASES, AT THEIR LOCATION. spark_landing / spark_mart (or
     <DBT_SCHEMA>_spark_*) are created with LOCATION <tables>/<schema> -- in `onelake`, the
     dbt lakehouse's Tables/, so a managed table lands exactly where every other engine's
     schema folder is.
  3. RE-REGISTER WHAT IS ALREADY THERE (onelake only). A fresh runner's metastore is EMPTY, so
     dbt-spark would see no relations, `is_incremental()` would be false on every run, and
     every incremental model would rebuild from scratch. Each Delta table already under those
     folders is registered first.
"""
from __future__ import annotations

import glob
import hashlib
import json
import os
import shutil
import sys
import tarfile
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"
TOKEN_REFRESH_SECONDS = 240

OUTPUTS = ("local", "onelake")

# The Apache release, checked against the sha512 Apache publishes beside it. dlcdn serves
# current releases only; archive.apache.org keeps every one, slower, so it is the fallback.
GLUTEN_VERSION = "1.7.0"
GLUTEN_TARBALL = f"apache-gluten-{GLUTEN_VERSION}-bin-spark-4.0.tar.gz"
GLUTEN_JAR = f"gluten-velox-bundle-spark4.0_2.13-linux_amd64-{GLUTEN_VERSION}.jar"
GLUTEN_SHA512 = ("81e692a7d2a136cdf7ade33a6c54385239eb45c8c4fe75b3667bca223cb075bf"
                 "ad0aec98a345509ec5fb3846e012dc974316bbd1c931c033605606ff4cf3c563")
GLUTEN_MIRRORS = (f"https://dlcdn.apache.org/gluten/{GLUTEN_VERSION}/{GLUTEN_TARBALL}",
                  f"https://archive.apache.org/dist/gluten/{GLUTEN_VERSION}/{GLUTEN_TARBALL}")

# process_limit in the local output: the warehouse starts empty, so "everything".
UNLIMITED = "1000000"


def output() -> str:
    out = os.environ.get("SPARK_OUTPUT", "").strip().lower() or "local"
    if out not in OUTPUTS:
        raise SystemExit(f"SPARK_OUTPUT={out!r}: expected one of {', '.join(OUTPUTS)}")
    return out


def gluten_enabled() -> bool:
    return os.environ.get("SPARK_GLUTEN", "1").strip().lower() not in ("0", "false", "no")


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


def gluten_jar() -> str:
    """The Velox bundle jar, downloaded once into ~/.cache and checked against GLUTEN_SHA512."""
    cache = Path(os.environ.get("GLUTEN_CACHE") or Path.home() / ".cache" / "gluten")
    cache = cache / GLUTEN_VERSION
    jar = cache / GLUTEN_JAR
    if jar.exists():
        return str(jar)
    cache.mkdir(parents=True, exist_ok=True)
    tarball = cache / GLUTEN_TARBALL
    for url in GLUTEN_MIRRORS:
        try:
            print(f"  fetching {url}", flush=True)
            with urllib.request.urlopen(url, timeout=600) as r, open(tarball, "wb") as f:
                shutil.copyfileobj(r, f)
            break
        except OSError as e:
            print(f"  ! {url}: {e}", flush=True)
    else:
        raise SystemExit(f"could not download {GLUTEN_TARBALL}; SPARK_GLUTEN=0 runs without it")
    h = hashlib.sha512()
    with open(tarball, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    if h.hexdigest() != GLUTEN_SHA512:
        tarball.unlink()
        raise SystemExit(f"{GLUTEN_TARBALL}: sha512 mismatch")
    with tarfile.open(tarball) as t:
        member = next(m for m in t.getmembers() if m.name.endswith("/" + GLUTEN_JAR))
        with t.extractfile(member) as src, open(jar, "wb") as dst:
            shutil.copyfileobj(src, dst)
    tarball.unlink()
    return str(jar)


def gluten_conf(jar: str) -> dict[str, str]:
    """The Gluten plugin and Velox's off-heap pool. local[*] runs the executor inside the
    driver JVM, so the driver's classpath is the one that matters. The off-heap pool is where
    Velox works and spills, and it comes ON TOP of the heap (SPARK_DRIVER_MEMORY): the two
    together must fit the runner."""
    return {
        "spark.plugins": "org.apache.gluten.GlutenPlugin",
        "spark.driver.extraClassPath": jar,
        "spark.executor.extraClassPath": jar,
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": os.environ.get("SPARK_OFFHEAP_SIZE", "6g"),
        "spark.shuffle.manager": "org.apache.spark.shuffle.sort.ColumnarShuffleManager",
        # The module docstring says why: an abfss:// scan would reach Velox's ABFS client.
        "spark.gluten.sql.columnar.filescan": "false",
        "spark.gluten.sql.columnar.batchscan": "false",
    }


def schemas() -> list[str]:
    """spark_landing / spark_mart under generate_schema_name()'s rule (deploy.mart_schema)."""
    sys.path.insert(0, str(SCRIPTS.parents[1]))
    from deploy import mart_schema

    mart = mart_schema("spark")
    return [mart[: -len("_mart")] + "_landing", mart]


def local_tables() -> str:
    """The local output's warehouse: EMPTIED every run, on a laptop as on the runner, so that
    lifting process_limit is always "the whole backlog against nothing"."""
    path = Path(os.environ.get("SPARK_LOCAL_TABLES")
                or os.path.join(tempfile.gettempdir(), "spark-tables")).resolve()
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True)
    return path.as_uri().rstrip("/")


def start() -> None:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    out = output()
    if out == "local":
        tables = local_tables()
        print(f"  SPARK_OUTPUT=local: nothing is written to OneLake; process_limit lifted from "
              f"{os.environ.get('process_limit', '(unset)')} to the whole backlog", flush=True)
        os.environ["process_limit"] = UNLIMITED
    else:
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
               # Spark 4 turned ANSI on by default; the module docstring says why it is off.
               .config("spark.sql.ansi.enabled", "false")
               # A dropped `<model>__stage` can leave its folder behind for a moment; a CTAS
               # into the same managed location must not refuse it.
               .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true"))
    for k, v in abfs.items():
        builder = builder.config(f"spark.hadoop.{k}", v)
    if gluten_enabled():
        for k, v in gluten_conf(gluten_jar()).items():
            builder = builder.config(k, v)
    hadoop = _hadoop_version()
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=[f"org.apache.hadoop:hadoop-azure:{hadoop}"]).getOrCreate()
    accel = f"gluten {GLUTEN_VERSION} (velox)" if gluten_enabled() else "no gluten"
    print(f"spark {spark.version}, hadoop {hadoop}, {accel}, output {out}, tables at {tables}",
          flush=True)

    import onelake

    for schema in schemas():
        location = f"{tables}/{schema}"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema} LOCATION '{location}'")
        if out == "local":
            continue
        registered = 0
        for name in onelake.list_dirs(location):
            if "_delta_log" not in onelake.list_dirs(f"{location}/{name}"):
                continue
            spark.sql(f"CREATE TABLE IF NOT EXISTS {schema}.`{name}` USING DELTA "
                      f"LOCATION '{location}/{name}'")
            registered += 1
        print(f"  {schema}: {registered} existing Delta tables registered", flush=True)
