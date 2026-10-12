"""Every script in the repo must at least COMPILE.

Most tests here read a script's source as text or import its pure functions, so a script that
does not parse can pass them all and fail only once a paid pipeline run reaches it -- which is
what happened to run_dbt.py (a literal newline inside a string, run 38199107673).

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import py_compile

import pytest

from _layout import REPO

SCRIPTS = sorted([*REPO.glob("*.py"), *(REPO / ".github" / "scripts").glob("*.py")])


@pytest.mark.parametrize("path", SCRIPTS, ids=lambda p: p.name)
def test_script_compiles(path, tmp_path):
    py_compile.compile(str(path), cfile=str(tmp_path / "out.pyc"), doraise=True)
