#!/usr/bin/env python3
"""Create the Fabric items one engine needs, and print the env vars its profile reads.

    python .github/scripts/provision.py <engine> >> "$GITHUB_ENV"

Idempotent: every item is create-if-missing, keep-if-present. Diagnostics go to stderr so
stdout is nothing but KEY=value lines.

    python .github/scripts/provision.py --reset-destination

is the one destructive call: it deletes the `dbt` lakehouse and the DuckLake catalog SQL DB
(DESTINATION, below) so the next provisioning recreates them empty. Never `dbt_landing`.
pipeline.yml runs it only when the `reset_destination` input is ticked (off by default).

THREE ITEMS, ALL INSIDE ONE WORKSPACE FOLDER. The engines used to get one data item each,
which put up to fourteen items at the root of a workspace that already has ~175 — and every
lakehouse drags an auto-created SQLEndpoint shadow item along with it. They share one
lakehouse now and are separated by SCHEMA instead (see macros/generate_schema_name.sql):

    <FOLDER>/
      dbt_landing        Lakehouse  the CSVs download_aemo.py lands. Written ONCE, read by all five.
      dbt                Lakehouse  duckrun_* iceberg_* ducklake_* spark_* polars_* schemas
                                    (Tables/), and the repo copy deploy.py puts in Files/dbt
      dbt_ducklake_meta  SQL DB     DuckLake catalog metadata only, no data

The SQL DB cannot collapse into the lakehouse: DuckLake's catalog must be a SQL database.
That is an engine-forced floor, not a layout choice.

THE LANDING PATH AND THE READ PATH ARE DIFFERENT VARIABLES, deliberately:

    LANDING_PATH  where download_aemo.py writes. IDENTICAL on all five legs, always
                  dbt_landing/Files. This is the whole basis of parity.py — if the engines
                  read different bytes, comparing their output means nothing.
    FILES_PATH    how THIS engine's dbt reads that zone. Same as LANDING_PATH today; a
                  separate key so an engine that must read it another way gets its own.

They used to be one variable, and provision.py re-emitted it for two engines — so those
two legs' downloaders landed their own private copies of the AEMO CSVs and the parity
comparison was quietly comparing different inputs.

EVERY ITEM THIS TOUCHES IS WRITTEN DOWN UNDER ITS GUID into the run-record fragment named by
`RUN_RECORD` (see record.py) -- plus, for ducklake, the catalog SQL DB its compute bills
against (`legs.<engine>.compute`). That is what measure_cu.py joins the Capacity Metrics
model on. `RUN_RECORD`
unset is a no-op, so running this by hand (or from the demo notebook) still works.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time

import requests

import record
import tokens

API = "https://api.fabric.microsoft.com/v1"
WS = os.environ["FABRIC_WORKSPACE_ID"]
FOLDER = os.environ.get("FOLDER", "dbt")

LANDING_LAKEHOUSE = "dbt_landing"
# The one lakehouse every Delta/Iceberg-writing engine shares. Schemas keep them apart.
DATA_LAKEHOUSE = "dbt"
DUCKLAKE_SQL_DB = "dbt_ducklake_meta"

LAKEHOUSE_ENGINES = {"duckrun", "iceberg", "ducklake", "spark", "polars"}
ENGINES = LAKEHOUSE_ENGINES


def log(msg: str) -> None:
    print(msg, file=sys.stderr)


def emit(key: str, value: str) -> None:
    print(f"{key}={value}")


FABRIC_RESOURCE = "https://api.fabric.microsoft.com"


def token(resource: str = FABRIC_RESOURCE) -> str:
    """A bearer token for `resource` -- tokens.py: notebookutils in Fabric, `az` elsewhere."""
    return tokens.get_token(resource)


def req(method: str, path: str, **kw):
    hdr = {"Authorization": f"Bearer {token()}", "Content-Type": "application/json"}
    for attempt in range(5):
        r = requests.request(method, f"{API}/{path}", headers=hdr, timeout=120, **kw)
        if r.status_code in (429, 500, 502, 503, 504) and attempt < 4:
            time.sleep(5 * 2**attempt)
            continue
        return r
    return r


def ensure_folder(name: str) -> str | None:
    """Create-if-missing, top level. Returns None if folders are unavailable."""
    r = req("GET", f"workspaces/{WS}/folders")
    if r.status_code == 200:
        for f in r.json().get("value", []):
            # Top-level only: a nested folder of the same name is a different folder.
            if f.get("displayName") == name and not f.get("parentFolderId"):
                log(f"  = folder {name} exists ({f['id']})")
                return f["id"]
    r = req("POST", f"workspaces/{WS}/folders", json={"displayName": name})
    if r.status_code in (200, 201):
        fid = r.json().get("id")
        log(f"  + created folder {name} ({fid})")
        return fid
    # Not fatal: items at the root still work, they are just untidy.
    log(f"  ! could not create folder {name}: {r.status_code} {r.text[:200]}")
    return None


def find(kind: str, name: str) -> str | None:
    r = req("GET", f"workspaces/{WS}/{kind}")
    if r.status_code != 200:
        return None
    for it in r.json().get("value", []):
        if it.get("displayName") == name:
            return it["id"]
    return None


def move_to_folder(item_id: str, folder_id: str) -> None:
    """Belt and braces: folderId on create is not honoured by every item type."""
    r = req("GET", f"workspaces/{WS}/items/{item_id}")
    if r.status_code == 200 and r.json().get("folderId") == folder_id:
        return
    r = req("POST", f"workspaces/{WS}/items/{item_id}/move",
            json={"targetFolderId": folder_id})
    if r.status_code not in (200, 201, 202):
        log(f"  ! could not move {item_id} into folder: {r.status_code} {r.text[:200]}")


NAME_NOT_AVAILABLE = "ItemDisplayNameNotAvailableYet"


def create(kind: str, body: dict):
    """POST a new item, waiting out the name Fabric keeps reserved for a while after a delete
    (409 ItemDisplayNameNotAvailableYet) -- which is exactly the state reset_destination()
    leaves `dbt` and the catalog SQL DB in."""
    for _ in range(40):
        r = req("POST", f"workspaces/{WS}/{kind}", json=body)
        if r.status_code != 409 or NAME_NOT_AVAILABLE not in r.text:
            return r
        log(f"  … {body['displayName']}: name not available yet after a delete; waiting")
        time.sleep(30)
    return r


# THE DESTINATION, AND ONLY THE DESTINATION. reset_destination() deletes these two items and
# nothing else: the shared `dbt` lakehouse (every engine's gold layer) and the DuckLake catalog
# SQL DB. The catalog has to go WITH the lakehouse: it records the lakehouse's abfss data_path on
# first attach and refuses a different one afterwards, and its snapshots name files that would
# no longer exist. `dbt_landing` -- the CSVs and the archive log every engine reads -- is never
# in this list; reset_destination() refuses to run if it ever is.
DESTINATION = (("lakehouses", DATA_LAKEHOUSE), ("sqlDatabases", DUCKLAKE_SQL_DB))


def reset_destination() -> None:
    """Delete the destination items so this run's provisioning recreates them empty. Every
    engine then does a first build -- the way to clear a gold layer that drifted from the
    landing zone in a way incremental runs cannot retract."""
    if LANDING_LAKEHOUSE in {name for _, name in DESTINATION}:
        raise SystemExit(f"refusing: {LANDING_LAKEHOUSE} is in DESTINATION")
    landing_id = find("lakehouses", LANDING_LAKEHOUSE)
    for kind, name in DESTINATION:
        item_id = find(kind, name)
        if not item_id:
            log(f"  = {kind[:-1]} {name} absent; nothing to delete")
            continue
        if item_id == landing_id:
            raise SystemExit(f"refusing: {name} resolved to the landing lakehouse ({item_id})")
        r = req("DELETE", f"workspaces/{WS}/items/{item_id}")
        if r.status_code not in (200, 202, 204):
            raise SystemExit(f"could not delete {kind[:-1]} {name} ({item_id}): "
                             f"{r.status_code} {r.text[:300]}")
        log(f"  - deleted {kind[:-1]} {name} ({item_id})")
        for _ in range(40):
            if not find(kind, name):
                break
            time.sleep(15)
        else:
            raise SystemExit(f"{kind[:-1]} {name} still listed after delete")


def ensure(kind: str, name: str, payload: dict | None = None,
           folder_id: str | None = None) -> str:
    """Create if missing, keep if present, and land it in the folder either way."""
    existing = find(kind, name)
    if existing:
        log(f"  = {kind[:-1]} {name} exists ({existing})")
        if folder_id:
            move_to_folder(existing, folder_id)
        return existing
    body = {"displayName": name}
    if payload:
        body.update(payload)
    if folder_id:
        body["folderId"] = folder_id
    r = create(kind, body)
    if r.status_code in (200, 201, 202):
        # Fabric answers 409 ItemDisplayNameNotAvailableYet for a while after a delete, and
        # the create itself can be async, so poll for the item rather than trusting the body.
        for _ in range(40):
            got = find(kind, name)
            if got:
                log(f"  + created {kind[:-1]} {name} ({got})")
                if folder_id:
                    move_to_folder(got, folder_id)
                return got
            time.sleep(15)
    raise SystemExit(f"could not create {kind[:-1]} {name}: {r.status_code} {r.text[:300]}")


def abfss(item_id: str, section: str) -> str:
    return f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{item_id}/{section}"


def main() -> int:
    if sys.argv[1:] == ["--reset-destination"]:
        log(f"resetting the destination in workspace {WS} ({LANDING_LAKEHOUSE} is kept)")
        reset_destination()
        return 0
    if len(sys.argv) != 2 or sys.argv[1] not in ENGINES:
        print(f"usage: provision.py [{' | '.join(sorted(ENGINES))} | --reset-destination]",
              file=sys.stderr)
        return 2
    engine = sys.argv[1]

    log(f"provisioning {engine} in workspace {WS}")
    folder_id = ensure_folder(FOLDER)
    record.item(folder_id, "folder", "Folder", FOLDER)

    landing_id = ensure("lakehouses", LANDING_LAKEHOUSE,
                        {"creationPayload": {"enableSchemas": True}}, folder_id)
    record.item(landing_id, "landing", "Lakehouse", LANDING_LAKEHOUSE)
    landing_path = abfss(landing_id, "Files")

    # Every engine's download_aemo.py writes HERE, and only here.
    emit("LANDING_PATH", landing_path)

    # The shared data lakehouse every engine writes its schemas into.
    data_id = ensure("lakehouses", DATA_LAKEHOUSE,
                     {"creationPayload": {"enableSchemas": True}}, folder_id)
    record.item(data_id, "data", "Lakehouse", DATA_LAKEHOUSE)
    # The item, not a path: the compact job and fabric mode address the lakehouse by GUID.
    emit("DATA_LAKEHOUSE_ID", data_id)

    if engine in LAKEHOUSE_ENGINES:
        # Reads the landing zone directly — no shortcut, no second copy.
        emit("FILES_PATH", landing_path)

    # The storage token the iceberg / ducklake / polars profiles read. NOT ON CI, for the same
    # reason as the SQL token below (stdout goes to $GITHUB_ENV unmasked; pipeline.yml mints it
    # with the mask). Off CI -- the deployed `run` notebook, a laptop -- this is what provides it.
    if engine in ("iceberg", "ducklake", "polars") and not os.environ.get("GITHUB_ACTIONS"):
        try:
            emit("ONELAKE_TOKEN", tokens.get_token("storage"))
        except (FileNotFoundError, subprocess.CalledProcessError):
            log("  ! no `az` to mint the storage token; export ONELAKE_TOKEN yourself")

    if engine == "duckrun":
        emit("ONELAKE_TABLES_PATH", abfss(data_id, "Tables"))

    elif engine == "iceberg":
        # The Iceberg REST catalog addresses the item as <workspace>/<item>, not abfss.
        emit("WAREHOUSE_PATH", f"{WS}/{data_id}")
        emit("ONELAKE_ENDPOINT", "https://onelake.table.fabric.microsoft.com/iceberg")

    elif engine == "polars":
        # The same Iceberg REST catalog as iceberg, through pyiceberg.
        emit("WAREHOUSE_PATH", f"{WS}/{data_id}")
        emit("ONELAKE_ENDPOINT", "https://onelake.table.fabric.microsoft.com/iceberg")

    elif engine == "ducklake":
        # The catalog metadata lives in a Fabric SQL DB. Created through the REST API, not
        # `fab create`, because only the API accepts a collation — and DuckLake needs a
        # UTF-8 one, which CANNOT be changed after creation.
        db_id = find("sqlDatabases", DUCKLAKE_SQL_DB)
        if not db_id:
            # `creationMode: New` IS REQUIRED. SQLDatabaseCreationPayload is a polymorphic
            # type (New | Restore | RestoreDeletedDatabase) and creationMode is its
            # discriminator, so a payload carrying only `collation` cannot be resolved and
            # the whole request comes back 400 InvalidInput -- with no hint that the
            # discriminator is what is missing. `collation` and `backupRetentionDays` are
            # accepted only under this mode.
            #
            # The collation must be a UTF-8 one for DuckLake and CANNOT be changed after
            # creation, which is why this goes through the REST API rather than `fab create`.
            body = {
                "displayName": DUCKLAKE_SQL_DB,
                "creationPayload": {
                    "creationMode": "New",
                    "collation": "Latin1_General_100_BIN2_UTF8",
                },
            }
            if folder_id:
                body["folderId"] = folder_id
            r = create("sqlDatabases", body)
            log(f"  + created SQL DB {DUCKLAKE_SQL_DB}: {r.status_code}")
            if r.status_code not in (200, 201, 202):
                # Fail here with the body rather than polling for something that is not coming.
                raise SystemExit(
                    f"could not create SQL DB {DUCKLAKE_SQL_DB}: {r.status_code} {r.text[:500]}"
                )
        server = database = ""
        for _ in range(40):  # provisioning is async and the connection details land last
            r = req("GET", f"workspaces/{WS}/sqlDatabases")
            if r.status_code == 200:
                for it in r.json().get("value", []):
                    if it.get("displayName") == DUCKLAKE_SQL_DB:
                        db_id = it["id"]
                        props = it.get("properties", {})
                        server = props.get("serverFqdn", "")
                        database = props.get("databaseName", "")
            if server and database:
                break
            time.sleep(15)
        if not (server and database):
            raise SystemExit("ducklake SQL DB connection details never appeared")
        if folder_id and db_id:
            move_to_folder(db_id, folder_id)
        record.item(db_id, "catalog", "SQLDatabase", DUCKLAKE_SQL_DB, engine="ducklake")
        # The catalog is COMPUTE, and it is ducklake's alone: a Fabric SQL DB bills `Sql Usage`
        # against its own item (19.5k CU over three days, measured
        # 2026-09-17), and nothing but the ducklake leg ever queries it. In Fabric mode the
        # notebook joins the same list; record.leg accumulates rather than replaces.
        record.leg("ducklake", compute=[db_id])
        emit("DUCKLAKE_CATALOG_DSN", f"Server={server};Database={database};Encrypt=yes")
        # DuckLake's parquet goes under the shared lakehouse's TABLES, as in the original
        # ducklake repo: DuckLake lays it out as <data_path>/<schema>/<table>/ and the
        # delta_export() on-run-end hook writes each table's _delta_log IN PLACE, so
        # Tables/ducklake_mart/<table> becomes a real lakehouse table Direct Lake can read.
        # (Under Files/ the export produced nothing Fabric could see.) The ducklake_* schema
        # folders keep it apart from duckrun_*, iceberg_* and spark_*. Its own key, NOT an
        # override of FILES_PATH: overriding that is what used to send this leg's downloader
        # off to a private copy of the CSVs.
        emit("DUCKLAKE_DATA_PATH", abfss(data_id, "Tables"))
        # NOT ON CI, even though the ducklake leg needs this token there: THIS
        # SCRIPT'S STDOUT IS REDIRECTED INTO $GITHUB_ENV, so anything emitted here never passes
        # through ::add-mask:: and would sit unmasked in the job's environment. pipeline.yml's
        # "Mint the DuckLake catalog token" step mints the same audience with the mask. Off CI
        # (a laptop, the deployed `run` notebook) this is the only thing that provides it:
        # tokens.py mints it with `az` there, or with notebookutils inside Fabric.
        if not os.environ.get("GITHUB_ACTIONS"):
            try:
                emit("DBT_ENV_SECRET_SQL_TOKEN", token("https://database.windows.net/"))
            except (FileNotFoundError, subprocess.CalledProcessError):
                log("  ! no `az` to mint the SQL DB token; export DBT_ENV_SECRET_SQL_TOKEN "
                    "yourself for a local ducklake run")

    elif engine == "spark":
        # Local PySpark (dbt-spark, method: session) on the runner: run_dbt.py creates the
        # spark_landing / spark_mart databases with their LOCATION here and registers the
        # Delta tables already in them, so a fresh runner's empty metastore still sees them.
        emit("SPARK_TABLES_PATH", abfss(data_id, "Tables"))

    log(f"provisioned {engine}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
