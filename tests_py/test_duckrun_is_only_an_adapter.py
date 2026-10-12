"""duckrun is the `duckrun` dbt target's adapter, and nothing else in this repo.

It used to be the scripts' Fabric toolkit too -- OIDC tokens, OneLake transport, deploy(),
get_stats(), and run_python to launch dbt in a throwaway notebook. That coupling is what broke
the polars leg (duckrun pins deltalake==1.5.0, Polars excludes 1.5.*: djouallah/duckrun#83), so
the scripts now use tokens.py, onelake.py, delta_stats.py and fabric-cicd, and this pins it.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re

from _layout import REPO

IMPORT = re.compile(r"^\s*(import duckrun|from duckrun|from dbt\.adapters\.duckrun)", re.M)


def _sources():
    for p in REPO.rglob("*"):
        if any(part in (".git", "target", "dbt_packages", "node_modules") for part in p.parts):
            continue
        if p.suffix in (".py", ".ipynb", ".yml", ".yaml") and p.is_file():
            yield p


def test_no_script_imports_duckrun():
    offenders = [str(p.relative_to(REPO)) for p in _sources()
                 if IMPORT.search(p.read_text(encoding="utf-8", errors="ignore"))]
    assert not offenders, f"duckrun imported outside the adapter: {offenders}"


def test_only_the_duckrun_engine_installs_it():
    for req in (REPO / "requirements").glob("*.txt"):
        lines = [l.split("#", 1)[0].strip() for l in req.read_text(encoding="utf-8").splitlines()]
        has = any(re.match(r"duckrun\b", l) for l in lines)
        assert has == (req.name == "duckrun.txt"), f"{req.name} installs duckrun: {has}"
