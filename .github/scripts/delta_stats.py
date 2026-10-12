"""Delta table statistics over one lakehouse's Tables/ -- what layout.py reads. A port of
duckrun's `get_stats()` (duckrun is now ONLY the dbt adapter), kept to the same two shapes so
layout.py's numbers stay comparable with every run already recorded:

  get_stats("<prefix>_*.*")            one row per table: catalog, schema, table, total_rows,
                                       num_files, num_row_groups, avg_row_group, size_mb,
                                       vorder, compression
  get_stats("schema.table", detailed=True)
                                       one row per (row group, column chunk): catalog, schema,
                                       table, then DuckDB's raw parquet_metadata() columns, with
                                       path_in_schema translated to the LOGICAL column name

The Delta log gives the live files (tombstones excluded), their bytes, the VORDER table property,
column mapping and deletion vectors; the parquet footers give the row-group shape. `total_rows`
is LOGICAL (deletion-vector rows subtracted, so it matches COUNT(*)); `avg_row_group` stays
PHYSICAL. Every table with a Delta log is covered: OneLake surfaces Iceberg tables with one too.
"""
from __future__ import annotations

import fnmatch

import onelake
import tokens

_PHYSICAL_NAME_KEY = "delta.columnMapping.physicalName"


def _q(s: str) -> str:
    return str(s).replace("'", "''")


def _children(dtype):
    kind = getattr(dtype, "type", None)
    if kind == "struct":
        return list(dtype.fields)
    if kind == "array":
        return _children(dtype.element_type)
    if kind == "map":
        return _children(dtype.key_type) + _children(dtype.value_type)
    return []


def physical_to_logical(dt) -> dict[str, str]:
    """{physical: logical} for a column-mapped table (Fabric Warehouse always, Spark sometimes):
    its footers are keyed `col-<guid>`. Empty when mapping is off; best-effort."""
    out: dict[str, str] = {}
    try:
        mode = str((dt.metadata().configuration or {}).get("delta.columnMapping.mode", "")).lower()
        if mode not in ("name", "id"):
            return {}
        pending = list(dt.schema().fields)
        while pending:
            f = pending.pop()
            physical = (f.metadata or {}).get(_PHYSICAL_NAME_KEY)
            if physical:
                out[str(physical)] = f.name
            pending.extend(_children(f.type))
    except Exception:  # noqa: BLE001
        return {}
    return out


def _logical_names(colmap: dict[str, str]) -> str:
    """The `* REPLACE` that maps parquet_metadata()'s path_in_schema back to logical names,
    segment by segment (DuckDB joins nested levels with ', '). `lambda y:`, not `y ->`: the
    pre-release DuckDB line rejects the arrow lambda."""
    if not colmap:
        return ""
    cases = " ".join(f"WHEN '{_q(p)}' THEN '{_q(l)}'" for p, l in colmap.items())
    return (" REPLACE (array_to_string(list_transform(str_split(m.path_in_schema, ', '), "
            f"lambda y: CASE y {cases} ELSE y END), ', ') AS path_in_schema)")


def _physical_column() -> str:
    """The footer's own name beside the translated one: anything that READS the files (layout's
    run-length scan) must select the physical `col-<guid>`, not the logical name."""
    return ", m.path_in_schema AS physical_path_in_schema"


class Tables:
    """One read-only view of `<root>` = an item's `.../Tables`."""

    def __init__(self, root: str, name: str = "tables"):
        self.root = root.rstrip("/")
        self.name = name
        self.con = onelake.duckdb_connect(self.root)
        self.storage_options = ({"bearer_token": tokens.get_token("storage")}
                                if onelake.is_remote(self.root) else {})

    # ---- discovery --------------------------------------------------------------------
    def _dirs(self, prefix: str = "") -> list[str]:
        import obstore

        res = obstore.list_with_delimiter(onelake.store(self.root), prefix or None)
        return [p.rstrip("/").rsplit("/", 1)[-1] for p in res["common_prefixes"]]

    def _targets(self, source: str) -> list[tuple[str, str]]:
        sch_pat, _, tbl_pat = source.rpartition(".")
        sch_pat = sch_pat or "*"
        out = []
        for sch in self._dirs():
            if fnmatch.fnmatch(sch.lower(), sch_pat.lower()):
                out += [(sch, t) for t in self._dirs(sch)
                        if fnmatch.fnmatch(t.lower(), tbl_pat.lower())]
        return out

    # ---- the Delta log half -------------------------------------------------------------
    def _summary(self, schema: str, table: str):
        from deltalake import DeltaTable

        path = f"{self.root}/{schema}/{table}"
        dt = DeltaTable(path, storage_options=self.storage_options or None)
        files = list(dt.file_uris())
        add_actions = dt.get_add_actions(flatten=True)  # noqa: F841 -- DuckDB replacement scan
        size = int(self.con.sql(
            "select coalesce(sum(size_bytes), 0)::bigint from add_actions").fetchone()[0])
        config = dt.metadata().configuration or {}
        vorder = str(config.get("delta.parquet.vorder.enabled", "")).strip().lower() == "true"
        deleted = 0
        try:
            if "deletionVectors" in (dt.protocol().reader_features or []):
                deletion_vectors = dt.deletion_vectors()  # noqa: F841 -- replacement scan
                deleted = int(self.con.sql(
                    "select coalesce(sum(len(selection_vector) - coalesce("
                    "list_aggregate(selection_vector, 'sum'), 0)), 0)::bigint "
                    "from deletion_vectors").fetchone()[0])
        except Exception:  # noqa: BLE001 -- an unreadable DV count is not worth a red read
            deleted = 0
        return files, size, vorder, deleted, physical_to_logical(dt)

    # ---- the duckrun-compatible entry point ----------------------------------------------
    def get_stats(self, source: str, detailed: bool = False):
        targets = (self._targets(source) if any(c in source for c in "*?[")
                   else [tuple(source.split(".", 1))])
        if not targets:
            raise ValueError(f"get_stats: nothing matches {source!r} under {self.root}")
        parts = []
        for sch, tbl in targets:
            files, size, vorder, deleted, colmap = self._summary(sch, tbl)
            pre = (f"'{_q(self.name)}' AS catalog, '{_q(sch)}' AS schema, "
                   f"'{_q(tbl)}' AS \"table\"")
            if not files:
                if not detailed:
                    parts.append(f"SELECT {pre}, 0 AS total_rows, 0 AS num_files, "
                                 f"0 AS num_row_groups, NULL::DOUBLE AS avg_row_group, "
                                 f"0.0 AS size_mb, {str(vorder).lower()} AS vorder, "
                                 f"'EMPTY' AS compression")
                continue
            lit = "[" + ", ".join(f"'{_q(f)}'" for f in files) + "]"
            if detailed:
                parts.append(f"SELECT {pre}, m.*{_logical_names(colmap)}{_physical_column()} "
                             f"FROM parquet_metadata({lit}) m")
            else:
                parts.append(
                    f"SELECT {pre}, "
                    f"COALESCE(SUM(rg_rows), 0) - {deleted} AS total_rows, "
                    f"{len(files)}::BIGINT AS num_files, "
                    f"COALESCE(SUM(rg_count), 0) AS num_row_groups, "
                    f"ROUND(SUM(rg_rows)::DOUBLE / NULLIF(SUM(rg_count), 0), 1) AS avg_row_group, "
                    f"ROUND({size} / 1048576.0, 2) AS size_mb, {str(vorder).lower()} AS vorder, "
                    f"COALESCE(STRING_AGG(compression, ', ' ORDER BY compression), 'UNCOMPRESSED') "
                    f"AS compression "
                    f"FROM (SELECT compression, "
                    f"SUM(row_group_num_rows) FILTER (WHERE column_id = 0) AS rg_rows, "
                    f"COUNT(*) FILTER (WHERE column_id = 0) AS rg_count "
                    f"FROM parquet_metadata({lit}) GROUP BY compression) g")
        if not parts:
            raise ValueError(f"get_stats: no files to describe for {source!r}")
        return self.con.sql(" UNION ALL ".join(parts))
