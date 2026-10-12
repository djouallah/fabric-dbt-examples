"""Parse every singular test's SQL in its OWN dialect.

The singular tests are the same assertion written five ways, and a naive port across
dialects is the classic way to end up with a test that parses on one engine and is
nonsense on another (Spark and DuckDB have no TOP; Spark wants `date`/`time` backticked).
dbt parse only checks the Jinja, never the SQL, and running the engines needs OneLake
credentials -- so sqlglot is the only offline check these get.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import sqlglot

from _layout import REPO, singular_tests_dir

DIALECT = {
    "duckrun": "duckdb",
    "iceberg": "duckdb",
    "ducklake": "duckdb",
    "spark": "spark",
    # Polars SQL is closest to DuckDB's; sqlglot has no polars dialect.
    "polars": "duckdb",
}


def strip_jinja(sql: str) -> str:
    """Replace dbt Jinja with something parseable, leaving the SQL shape intact."""
    sql = re.sub(r"\{\{\s*config\([^}]*?\)\s*\}\}", "", sql, flags=re.S)
    # {{ ref('x') }} -> a plain identifier
    sql = re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}", r"\1", sql)
    sql = re.sub(r"\{\{\s*this\s*\}\}", "this_table", sql)
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    sql = re.sub(r"\{%.*?%\}", "", sql, flags=re.S)
    sql = re.sub(r"\{\{.*?\}\}", "1", sql, flags=re.S)
    return sql.strip()


def cases():
    for engine, dialect in DIALECT.items():
        for p in sorted(singular_tests_dir(engine).glob("*.sql")):
            yield pytest.param(p, dialect, id=f"{engine}/{p.stem}")


@pytest.mark.parametrize("path,dialect", list(cases()))
def test_singular_test_parses_in_its_dialect(path, dialect):
    sql = strip_jinja(path.read_text(encoding="utf-8"))
    assert sql, f"{path} rendered empty"
    try:
        sqlglot.parse_one(sql, dialect=dialect)
    except Exception as e:  # noqa: BLE001 - sqlglot raises several types
        pytest.fail(f"{path.relative_to(REPO)} is not valid {dialect}:\n{e}\n\n{sql}")


@pytest.mark.parametrize("path,dialect", list(cases()))
def test_no_wrong_dialect_idioms(path, dialect):
    """sqlglot is lenient about some cross-dialect idioms, so check the known traps by hand."""
    # Comments are prose and mention "date"/"time" freely; only look at executable SQL.
    sql = re.sub(r"--[^\n]*", "", path.read_text(encoding="utf-8"))
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    assert not re.search(r"\bSELECT\s+TOP\s+\d", sql, re.I), f"{dialect} has no SELECT TOP"
    if dialect == "spark":
        # `date` and `time` are reserved-ish in Spark SQL and must be backticked.
        for col in ("date", "time"):
            bare = re.search(r"(?<![`\w.])" + col + r"(?![`\w])", sql)
            assert not bare, f"bare `{col}` in Spark SQL must be backticked: {bare.group(0)!r}"
