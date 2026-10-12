#!/usr/bin/env python3
"""Deploy to Fabric with fabric-cicd: the repo into the shared lakehouse, the run items, and a
Direct Lake semantic model per engine. One half of the Fabric mode (the other is running the
deployed pipeline, .github/scripts/fabric_run.py); see AGENTS.md "Two ways to run".

Run by pipeline.yml after `azure/login` (fabric-cicd and tokens.py both use the CLI login).
On a laptop, after `az login`:

    DEPLOY_ENGINES=iceberg python deploy.py

Env-driven, no argv:

    FABRIC_WORKSPACE_ID    required
    DEPLOY_PARTS           comma list of: repo, items, models, schedule (default: all four)
    DEPLOY_ENGINES         engines to provision and bind a model for (default: all)
    DEPLOY_SEMANTIC_MODEL  "false" drops `models` from DEPLOY_PARTS (the pipeline's no_model)
    DBT_SCHEMA             the schema the engines were built with (default mart), see mart_schema()
    FOLDER                 the workspace folder every item lands in (default dbt)

WHAT IT DOES, IN ORDER
  1. .github/scripts/provision.py <engine>, per engine: every Fabric item exists (idempotent).
  2. repo: the git-tracked repo goes to Files/dbt/ of the shared `dbt` lakehouse, stale files
     removed. That copy is what fabric_items/run.Notebook runs: the SAME provision / land /
     build scripts the local mode runs.
  3. items: fabric_items/ (variable library, notebook, pipeline) through fabric-cicd's
     publish_all_items. fabric_items/parameter.yml points the pipeline's activities at THIS
     workspace; fabric-cicd itself resolves the notebook's logical id to the deployed item.
  4. models: the semantic model, once per engine, as aemo_<engine> bound to <engine>_mart.
  5. schedule: the pipeline's 12-hourly schedule (Fabric REST job scheduler).

DIRECT LAKE HAS NO SCHEMA PARAMETER. A partition's schemaName is a literal in the model
definition, so the committed model.bim carries placeholders -- `mart`, and Desktop's workspace /
lakehouse GUIDs in the OneLake expression -- and bind_model() writes the engine's schema and
item in at deploy time. Every engine writes <engine>_mart (macros/generate_schema_name.sql);
DBT_SCHEMA prefixes it here exactly as it does there.

ORDER MATTERS: a Direct Lake model is REFRAMED on deploy, which needs <schema>.fct_summary to
exist -- which is why `models` is deployed after the build.

Every engine writes into one shared lakehouse, separated by schema, so the models differ in
that one string only.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parent
SCRIPTS = REPO / ".github" / "scripts"
MODEL_DIR = REPO / "semantic_model" / "aemo_electricity.SemanticModel"
BIM = MODEL_DIR / "model.bim"
ENGINES = ("duckrun", "iceberg", "ducklake", "spark", "polars")
PARTS = ("repo", "items", "models", "schedule")

# Desktop's GUIDs in model.bim's OneLake expression: <workspace>/<lakehouse>.
BIM_WORKSPACE = "e446a5e7-6666-42ad-a331-0bfef3187fbf"
BIM_ITEM = "cf8bbf8a-5488-4020-ac8f-293727b447b1"

WS = os.environ.get("FABRIC_WORKSPACE_ID", "")
FOLDER = os.environ.get("FOLDER", "dbt")
DEPLOY_ENGINES = [e.strip() for e in os.environ.get("DEPLOY_ENGINES", ",".join(ENGINES)).split(",")
                  if e.strip()]
DEPLOY_PARTS = [p.strip() for p in os.environ.get("DEPLOY_PARTS", ",".join(PARTS)).split(",")
                if p.strip()]
if os.environ.get("DEPLOY_SEMANTIC_MODEL", "true").lower() == "false":
    DEPLOY_PARTS = [p for p in DEPLOY_PARTS if p != "models"]
PIPELINE = "run_pipeline"
SCHEDULE_MINUTES = 720


def log(m: str) -> None:
    print(m, flush=True)


def mart_schema(engine: str) -> str:
    """The schema this engine's gold layer lives in -- the rule of generate_schema_name():
    DBT_SCHEMA unset or 'mart' -> <engine>_mart, anything else -> <DBT_SCHEMA>_<engine>_mart."""
    prefix = os.environ.get("DBT_SCHEMA", "mart")
    return f"{engine}_mart" if prefix == "mart" else f"{prefix}_{engine}_mart"


def rebind_schema(bim_text: str, schema: str) -> str:
    """Point every Direct Lake partition (and its table's lineage tag) at `schema`.

    Walks the JSON rather than replacing text, so a reformatted bim cannot leave a partition
    silently on the `mart` placeholder -- a model bound to a schema no engine writes."""
    model = json.loads(bim_text)
    for table in model["model"].get("tables", []):
        for part in table.get("partitions", []):
            src = part.get("source", {})
            if "schemaName" in src:
                src["schemaName"] = schema
        tag = table.get("sourceLineageTag", "")
        if tag.startswith("[") and "].[" in tag:
            table["sourceLineageTag"] = f"[{schema}].[" + tag.split("].[", 1)[1]
    return json.dumps(model, indent=2) + "\n"


def bind_model(bim_text: str, schema: str, workspace_id: str, item_id: str) -> str:
    """rebind_schema, plus the OneLake expression pointed at <workspace_id>/<item_id>."""
    if f"{BIM_WORKSPACE}/{BIM_ITEM}" not in bim_text:
        raise SystemExit("model.bim no longer carries the placeholder OneLake GUIDs deploy.py "
                         "rewrites -- update BIM_WORKSPACE / BIM_ITEM")
    bound = bim_text.replace(f"{BIM_WORKSPACE}/{BIM_ITEM}", f"{workspace_id}/{item_id}")
    return rebind_schema(bound, schema)


# ---------------------------------------------------------------------------------- parts

def copy_repo(provision) -> None:
    """`git ls-files` into Files/dbt/ of the shared lakehouse; stale remote files removed."""
    import onelake

    files = subprocess.run(["git", "ls-files", "-z"], cwd=REPO, check=True,
                           capture_output=True).stdout.decode().split("\0")
    files = [f for f in files if f and (REPO / f).is_file()]
    data_id = provision.find("lakehouses", provision.DATA_LAKEHOUSE)
    base = f"{provision.abfss(data_id, 'Files')}/dbt"
    with tempfile.TemporaryDirectory() as tmp:
        for f in files:
            dst = Path(tmp) / f
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(REPO / f, dst)
        n = onelake.put_dir(tmp, base, overwrite=True)
    stale = sorted(set(onelake.list_keys(base)) - set(files))
    for key in stale:
        try:
            onelake.delete(base, key)
        except FileNotFoundError:
            pass
    log(f"copied {n} files into {provision.DATA_LAKEHOUSE}/Files/dbt ({len(stale)} stale removed)")


def publish(stage: Path, item_types: list[str]) -> None:
    from fabric_cicd import FabricWorkspace, publish_all_items

    ws = FabricWorkspace(workspace_id=WS, environment="ci", repository_directory=str(stage),
                         item_type_in_scope=item_types)
    publish_all_items(ws)


def move_into_folder(provision, names: list[tuple[str, str]]) -> None:
    """fabric-cicd publishes at the workspace root; the items live in FOLDER, beside the
    lakehouses provision.py made. Idempotent."""
    folder_id = provision.ensure_folder(FOLDER)
    if not folder_id:
        return
    for kind, name in names:
        item_id = provision.find(kind, name)
        if item_id:
            provision.move_to_folder(item_id, folder_id)


def deploy_items(provision) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        stage = Path(tmp)
        shutil.copytree(REPO / "fabric_items", stage, dirs_exist_ok=True)
        publish(stage, ["VariableLibrary", "Notebook", "DataPipeline"])
    move_into_folder(provision, [("notebooks", "run"), ("dataPipelines", PIPELINE),
                                 ("variableLibraries", "deploy_config")])
    log("deployed fabric_items")


def deploy_models(provision) -> None:
    data_id = provision.find("lakehouses", provision.DATA_LAKEHOUSE)
    names = []
    with tempfile.TemporaryDirectory() as tmp:
        stage = Path(tmp)
        for engine in DEPLOY_ENGINES:
            schema = mart_schema(engine)
            item_id = data_id
            name = f"aemo_{engine}"
            d = stage / f"{name}.SemanticModel"
            shutil.copytree(MODEL_DIR, d)
            (d / "model.bim").write_text(
                bind_model(BIM.read_text(encoding="utf-8-sig"), schema, WS, item_id),
                encoding="utf-8")
            platform = json.loads((d / ".platform").read_text(encoding="utf-8"))
            platform["metadata"]["displayName"] = name
            platform["config"]["logicalId"] = str(uuid.uuid5(uuid.NAMESPACE_URL, name))
            (d / ".platform").write_text(json.dumps(platform, indent=2) + "\n", encoding="utf-8")
            names.append(("semanticModels", name))
            log(f"semantic model {name} over {schema}")
        publish(stage, ["SemanticModel"])
    move_into_folder(provision, names)


def schedule(provision) -> None:
    """The pipeline's schedule, through the Fabric job scheduler. Updated in place when one
    exists, so redeploys do not stack duplicates."""
    from datetime import datetime, timedelta, timezone

    pid = provision.find("dataPipelines", PIPELINE)
    if not pid:
        raise SystemExit(f"{PIPELINE} is not deployed; deploy `items` first")
    start = datetime.now(timezone.utc).replace(microsecond=0)
    body = {"enabled": True, "configuration": {
        "type": "Cron", "interval": SCHEDULE_MINUTES, "localTimeZoneId": "UTC",
        "startDateTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "endDateTime": (start + timedelta(days=3650)).strftime("%Y-%m-%dT%H:%M:%SZ")}}
    path = f"workspaces/{WS}/items/{pid}/jobs/Pipeline/schedules"
    existing = provision.req("GET", path).json().get("value", [])
    if existing:
        provision.req("PATCH", f"{path}/{existing[0]['id']}", json=body)
    else:
        provision.req("POST", path, json=body)
    log(f"{PIPELINE} scheduled every {SCHEDULE_MINUTES} minutes")


def main() -> int:
    if not WS:
        raise SystemExit("FABRIC_WORKSPACE_ID is not set")
    unknown = sorted(set(DEPLOY_ENGINES) - set(ENGINES))
    if unknown:
        raise SystemExit(f"DEPLOY_ENGINES: unknown engine(s) {unknown}; choose from {ENGINES}")
    bad = sorted(set(DEPLOY_PARTS) - set(PARTS))
    if bad:
        raise SystemExit(f"DEPLOY_PARTS: unknown part(s) {bad}; choose from {PARTS}")

    # provision.py reads FABRIC_WORKSPACE_ID at import, hence after the check.
    sys.path.insert(0, str(SCRIPTS))
    import provision  # noqa: E402

    for engine in DEPLOY_ENGINES:
        log(f"provisioning {engine}")
        subprocess.run([sys.executable, str(SCRIPTS / "provision.py"), engine],
                       check=True, stdout=subprocess.DEVNULL, cwd=REPO)

    if "repo" in DEPLOY_PARTS:
        copy_repo(provision)
    if "items" in DEPLOY_PARTS:
        deploy_items(provision)
    if "models" in DEPLOY_PARTS:
        deploy_models(provision)
    if "schedule" in DEPLOY_PARTS:
        schedule(provision)
    log("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
