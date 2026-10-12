"""Bearer tokens for the scripts, from wherever they run. The ONE place a script gets a token.

Inside a Fabric notebook (the deployed `run` notebook runs these scripts there) notebookutils
mints any audience for the notebook's identity, and there is no `az`. Everywhere else -- a
GitHub Actions job after `azure/login`, or a laptop after `az login` -- the Azure CLI does.

duckrun used to mint some of these from the GitHub OIDC assertion, which is why several jobs
had no login step. duckrun is now ONLY the dbt adapter (AGENTS.md); every job that touches
Azure logs in.
"""
from __future__ import annotations

import subprocess

AUDIENCE = {
    "storage": "https://storage.azure.com/",
    "fabric": "https://api.fabric.microsoft.com",
    "sql": "https://database.windows.net/",
    "powerbi": "https://analysis.windows.net/powerbi/api",
}

# notebookutils names the storage audience 'storage'; every other one by its resource URL.
_NOTEBOOK_NAME = {"storage": "storage"}


def get_token(audience: str) -> str:
    """A bearer token for `audience`: a key of AUDIENCE, or a resource URL."""
    resource = AUDIENCE.get(audience, audience)
    try:
        import notebookutils  # type: ignore

        return notebookutils.credentials.getToken(_NOTEBOOK_NAME.get(audience, resource))
    except ImportError:
        pass
    return subprocess.run(
        ["az", "account", "get-access-token", "--resource", resource,
         "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
