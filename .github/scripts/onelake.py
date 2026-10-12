"""OneLake (or a local directory) for the repo's scripts: a DuckDB session that can read it,
and file transfer into it. What duckrun's `connect()` / `copy()` used to provide here, now
that duckrun is ONLY the dbt adapter.

A base without a scheme is a local directory -- the local recipe in AGENTS.md, and the
offline tests. `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>/<section>/...` is
OneLake, authorised with one storage bearer token from tokens.py.

Two OneLake facts this module is shaped by, both learned in duckrun (arrow-rs object_store
#701): obstore's Azure DELETE is a BATCH request, which OneLake rejects ("Either WorkspaceId
or ArtifactId are missing") -- so a delete here is one raw ADLS Gen2 `Path - Delete` per key;
and a plain PUT already replaces an existing object, so an overwrite needs no delete first.
"""
from __future__ import annotations

import os
from urllib.parse import quote, urlparse

import tokens

DFS_API_VERSION = "2023-11-03"


def is_remote(base: str) -> bool:
    return "://" in base


def duckdb_connect(base: str):
    """A DuckDB connection that can read `base`: httpfs + json always, and for OneLake the
    azure extension with an `access_token` secret -- the shape dbt1/profiles.yml's iceberg
    output uses. The curl transport is honoured when the job sets it (the runner's TLS)."""
    import duckdb

    con = duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs; INSTALL json; LOAD json;")
    if is_remote(base):
        con.sql("INSTALL azure; LOAD azure;")
        transport = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE", "default")
        if transport != "default":
            con.sql(f"SET azure_transport_option_type = '{transport}'")
        token = tokens.get_token("storage").replace("'", "''")
        con.sql(f"CREATE OR REPLACE SECRET onelake (TYPE azure, PROVIDER access_token, "
                f"ACCESS_TOKEN '{token}')")
    return con


def store(base: str):
    """An obstore store rooted at `base` (created locally if absent)."""
    import obstore.store as st

    if not is_remote(base):
        os.makedirs(base, exist_ok=True)
        return st.LocalStore(prefix=os.path.abspath(base))
    return st.from_url(base, config={"bearer_token": tokens.get_token("storage")})


def join(base: str, rel: str) -> str:
    return f"{base.rstrip('/')}/{rel.strip('/')}" if rel else base.rstrip("/")


def put_dir(local_folder: str, base: str, overwrite: bool = True) -> int:
    """Upload every file under `local_folder` to `base`, keeping relative paths. With
    overwrite=False an object already there is left alone (a HEAD per file). Returns the
    number of files written."""
    import obstore

    s = store(base)
    n = 0
    for root, _dirs, files in os.walk(local_folder):
        for name in files:
            path = os.path.join(root, name)
            key = os.path.relpath(path, local_folder).replace(os.sep, "/")
            if not overwrite:
                try:
                    obstore.head(s, key)
                    continue
                except FileNotFoundError:
                    pass
            with open(path, "rb") as f:
                obstore.put(s, key, f)
            n += 1
    return n


def list_keys(base: str) -> list[str]:
    """Every object key under `base`, recursively, relative to it."""
    import obstore

    keys: list[str] = []
    for batch in obstore.list(store(base)):
        keys.extend(obj["path"] for obj in batch)
    return keys


def list_dirs(base: str) -> list[str]:
    """The immediate sub-folders of `base` (names only). [] when `base` does not exist."""
    import obstore

    try:
        res = obstore.list_with_delimiter(store(base))
    except FileNotFoundError:
        return []
    return [p.rstrip("/").rsplit("/", 1)[-1] for p in res["common_prefixes"]]


def delete(base: str, key: str) -> None:
    """Delete one object. On OneLake a raw DFS DELETE (see the module docstring)."""
    if not is_remote(base):
        os.remove(os.path.join(base, key))
        return
    import requests

    u = urlparse(join(base, key))
    filesystem, host = u.netloc.split("@", 1)
    url = f"https://{host}/{filesystem}/" + quote(u.path.lstrip("/"), safe="/")
    r = requests.delete(url, timeout=60, headers={
        "Authorization": f"Bearer {tokens.get_token('storage')}",
        "x-ms-version": DFS_API_VERSION,
    })
    if r.status_code == 404:
        raise FileNotFoundError(key)
    r.raise_for_status()
