# Candidate engines

Two engines have been looked at as a sixth leg. **Polars became it**: dbt-polars runs the gold
layer from a fork that carries the two adapter fixes it needed (see AGENTS.md, "The polars
engine"). **Sail** is still not built: the engine works and its *packaging* does not.

The Polars section below is kept as the record of how the leg was scoped. Both are recorded
here rather than dropped, because "we tried it and this is what stopped it"
is the part that goes missing. A candidate engine's gaps are worked around inside its own leg,
never by changing the working five — so what matters about each is exactly which gaps a leg
would have to carry.

---

## Sail — the engine passes, the packaging does not

**Sail** ([LakeSail](https://github.com/lakehq/sail)) was evaluated as a sixth and **is not
being built**, for a reason that has nothing to do with the engine: `dbt-sail`'s declared
dependency is `dbt-spark[session]`, which pulls **full PySpark with its jars** (and conflicts
with `pyspark-client`). Shipping the entire Spark distribution in order to avoid Spark is the
opposite of the point — a JVM need never run, but it is in the image. That is a packaging
choice, fixable upstream, so the evaluation below stands for whenever it is.

The engine itself is a Rust Spark replacement with no JVM, and its native OneLake catalog
takes the same bearer token the `iceberg` leg mints, so a sail leg would be a second Iceberg
writer against the same gold layer.
`.github/workflows/sail_smoke.yml` probes it — `workflow_dispatch` only, gates nothing — in
two phases, because "the catalog accepts SQL" and "this repo's models could run on it" are
different claims.

**Phase A, the adapter contract: all green** on Sail 0.7.1. Schema and table creation, insert,
both `MERGE INTO` shapes dbt-spark emits, `show table extended` (how dbt-spark decides an
incremental model exists), and a read-back proving the merges applied rather than merely
returning.

**Phase B, what the models actually do: everything, bar one design decision.** Sail reads the real
ragged AEMO CSVs off OneLake — 666k rows across two `PUBLIC_DAILY` files — keeps one record
type, parses the slash date, counts DUIDs, and does `sequence()`/`explode()`, window
functions, a 130-column record, a multi-column merge key and a genuinely temporary view.

Reading a ragged file takes two things **together**, and either alone fails:

| | |
|---|---|
| a schema **padded to at least the widest record** in the file | a `PUBLIC_DAILY` holds many record types — DUNIT is 53 columns, DREGION 130 — so a narrower schema is `expected 4, got 10` |
| **`allowTruncatedRows`** for every row narrower than that | without it the padded schema fails the other way: `expected 131, got 14` |

`mode 'PERMISSIVE'`, which is how the spark leg says this, does **not** do it on Sail. The
probe keeps a control (probe 24) that is identical but for the option, so the read's success
is attributable to it rather than to the padding.

**The one Sail limitation: it cannot name the source file.** `input_file_name()` is
`UnsupportedOperationException` and `_metadata.file_name` is `cannot resolve attribute`
([lakehq/sail#1210](https://github.com/lakehq/sail/issues/1210), open). Every other engine
here has one — DuckDB's `filename`, Fabric's `src.filepath()`, Spark's `input_file_name()`,
which is why `macros/parse_filename.sql` has a dialect branch at all.

Not a blocker: `file` is a *choice* of `unique_key`, and within a `source_type`
`(DUID|REGIONID, SETTLEMENTDATE, INTERVENTION)` is already the natural grain, so a sail leg
keys its facts on those instead. That is the sail leg's own workaround for a Sail gap —
the same kind of per-engine operational difference this repo already carries five of.

A direct ``csv.`path`` read also fails, since that form carries no options and so cannot pass
`allowTruncatedRows` — but the view form is what `spark_read_csv.sql` emits anyway.

Two dialect facts fell out as well: Sail rounds `DOUBLE` → `DECIMAL` **HALF_UP** (like Spark,
unlike DuckDB), and a bare `CAST` of AEMO's `yyyy/MM/dd` is a hard parse error rather than
Spark's silent `NULL` — strictly better, since it cannot reach the gold layer unnoticed.

Three things the probe had to get right before any of the above was measurable, each
found by a failed run and each a cost a real leg would carry:

| what | why |
|---|---|
| the catalog url as `<workspace-id>/<lakehouse-id>` | a name comes back `Failed to load config: 400 Bad Request` |
| `AZURE_STORAGE_TOKEN` as well as the catalog's `bearer_token` | the catalog token authorises the CATALOG; the data files are a separate credential. The same split `dbt1/profiles.yml` spells out for the `iceberg` leg, which brings both too. Without it Sail falls through to the instance metadata endpoint |
| `USING iceberg` + `tblproperties('write.merge.mode'='merge-on-read')` | Sail's `CREATE TABLE` defaults to parquet, which the REST catalog refuses; and it will not merge a copy-on-write table. dbt-spark spells both in config, on every merged model |

---

## Polars — built, as the `polars` engine

What the probes found, in order. The answer to each blocker is now in the leg itself.

**There is an adapter now:** [dbt-polars](https://github.com/datamindedbe/incubator-dbt-polars)
0.3.1 on PyPI, dbt-core 1.x. It runs a SQL model with Polars' own SQL engine (`pl.SQLContext`),
and writes Iceberg with pyiceberg. `.github/workflows/polars_smoke.yml` runs it for real:
`workflow_dispatch` only, gates nothing, **SQL models only**. A Python model would test a
rewrite of the gold layer, not the engine. Every write is checked by reading the table back.

Run [38045478706](https://github.com/djouallah/fabric-medallion-dbt-community/actions/runs/38045478706)
(dbt-polars 0.3.1, polars 2.0.0, pyiceberg 0.11.1), OneLake Iceberg REST catalog:

| | result |
|---|---|
| table model, built twice | works: one append, then one overwrite |
| incremental `merge`, three-column key | works. It is two commits: a delete (`overwrite`), then an `append` |
| insert-only merge (`merge_clauses` `do_nothing`) | **wrong result, no error.** `merge_clauses` is ignored and the matched row is updated |
| `append` + `NOT IN {{ this }}` | works |
| `pre_hook` with SQL | **error**: "dbt-polars does not support SQL hooks: there is no database to run them on" |
| `run_query` at parse time | works |
| landed AEMO CSV with Polars SQL `read_csv('abfss://…/X.CSV')` | works (550,375 rows); the token comes from `AZURE_STORAGE_TOKEN` |
| the same file as a dbt source | **fails.** The adapter adds `.csv` in lower case, and AEMO files end in `.CSV` ([dbt-polars#29](https://github.com/datamindedbe/incubator-dbt-polars/issues/29)). A folder glob matches nothing for the same reason |
| slash date (`strptime`, and a plain `CAST`) | works |
| `CAST(… AS TIMESTAMPTZ)` | **error**: timestamp with time zone not supported |
| `unnest(generate_series(…))` (dim_calendar) | **error**: unsupported function. `{{ dbt.date_spine() }}` works |
| window function, `DOUBLE → DECIMAL`, `split_part(…, -1)` | works. `0.125` → `DECIMAL(18,2)` gives `0.12`, like DuckDB |
| `{{ dbt.hash() }}` | **error**: Polars SQL has no hash function; the adapter says to use a Python model |

Run [38051227050](https://github.com/djouallah/fabric-medallion-dbt-community/actions/runs/38051227050)
(same versions) probed the gaps that run left open:

| | result |
|---|---|
| Polars SQL `read_csv(path, has_header => false, …)` | **error**: "`read_csv` expects a single file path; found 6 arguments". Polars SQL `read_csv()` takes **no options at all** |
| `select *` from a bare `read_csv()` on a ragged AEMO file | **error**: "found more fields than defined in 'Schema'". It infers the schema from the first line and cannot be told otherwise |
| `read_csv()` over a `*.CSV` glob | **error**: "schema names differ" between files, for the same reason |
| two files as `UNION ALL` of `read_csv()` | works, 1,100,750 rows = the padded reads, but only because the probe selects **no column**: a bare `count(*)` never materialises the ragged fields |
| file picker at parse time: `run_query` on `{{ this }}`, keys rendered as a literal `NOT IN` | works. No `pre_hook`, no `merge_clauses`, insert-only result correct |
| `run_query` over a landed file at parse time | works (550,375): the parse-time context reaches OneLake |
| `CAST(… AS TIMESTAMP WITH TIME ZONE)`, `… AT TIME ZONE 'UTC'` | **error**, both: no tz-aware timestamp in Polars SQL by any spelling |

Run [38121051083](https://github.com/djouallah/fabric-medallion-dbt-community/actions/runs/38121051083)
installs dbt-polars **from the fork** at the head of
[dbt-polars#30](https://github.com/datamindedbe/incubator-dbt-polars/pull/30), the fix for #29
(`requirements/polars_smoke.txt` pins the commit until a release carries it):

| | result |
|---|---|
| a source naming one `.CSV` file | **reads.** DUNIT rows 164,448 = the padded read |
| a `*.CSV` glob source, filtered on `filename` to two files | **reads**, 328,896 DUNIT rows = the padded reads, and well inside the timeout over a folder of tens of GB |
| the width of either read | **25 columns**, the width of the file's first data line. A DUNIT record is 53 fields, so every one is **cut short** by `truncate_ragged_lines` |
| `read_options: {schema: {column_1: String, …}}` to pad it | **error**: "'str' is not a Polars data type". dbt-polars hands `read_options` to `scan_csv` unconverted, and YAML can only carry the type's name. One such source fails every model that reads *any* source |
| column names of a headerless read | `column_0 …`: **0-based** in Polars 2.0, where every other reader here is 1-based |

So #30 removes the `.CSV` blocker and the source becomes the way in. It also gives the file list
for free, since a `filename` filter on one glob source selects the files. The remaining wall is
the **width**: a source cannot be told the 131-column schema, so the wide records cannot be read
whole. That is one more small adapter change: convert dtype names in `read_options` (`schema`,
`schema_overrides`) to Polars types.

**What blocks a leg.** Two of the three blockers have a hook-free workaround now. The file
picker can move to parse time (a `run_query` against `{{ this }}`), and that also gives an
insert-only load through `append`, so neither SQL hooks nor `merge_clauses` is needed. A
rendered file list can be read as a `UNION ALL`.

**The read itself is the wall.** A ragged AEMO file needs the reader told the width and the
options (no header, all String, `truncate_ragged_lines`, the file name). Polars SQL
`read_csv()` accepts a path and nothing else. A dbt source *can* carry those options, but it
cannot name a `.CSV` file or a list of files
([dbt-polars#29](https://github.com/datamindedbe/incubator-dbt-polars/issues/29)). So no SQL
model can read the fact models' input today. Either Polars SQL `read_csv()` gains options,
or #29 is fixed and a source takes a file list. Then `TIMESTAMPTZ` and `generate_series`
would still need a Polars branch of their own (`date_spine` covers the second; the first would
make `SETTLEMENTDATE` tz-naive on this leg alone, which `parity.py` would have to allow).

So the adapter is no longer the blocker to *evaluating* Polars. Its CSV read is the blocker
to *adopting* it.
