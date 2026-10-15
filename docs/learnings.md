# Learnings — the SQL ports, the output is what you check

Five engines run one gold layer here, and porting the SQL between them was the easy part: write
it once, and an AI assistant will translate it to Spark SQL, Polars SQL or T-SQL in seconds.
What it cannot do is tell you the translation *means* the same thing. Every story below shipped
with **every dbt test green**. They were caught one way only: by comparing what the engines
*produced*.

[Engine nuances](engine-nuances.md) lists the differences as they stand today. This page is how
they were found.

## Same SQL, different answer

- **Spark compares a string to a number by truncating the string.** The csv stage is
  all-`STRING`, and `SCADAVALUE != 0` became `CAST('0.5' AS INT) != 0` — false. Spark dropped
  every row with `0 < |value| < 1`: 12–16% of the intraday SCADA rows, a `fct_summary` tail
  200–1,500 rows short. Every test passed; only parity saw it (run 35188161001). Fix: an
  explicit `CAST(... AS DOUBLE)`, pinned by `tests_py/test_spark_stage_filter.py`.
- **Spark's `CAST(string AS TIMESTAMP)` returns `NULL` for `2026/09/17`** instead of erroring.
  AEMO ships slashes. DuckDB parses them, so one engine silently nulled a whole timestamp
  column. Fix: parse the format explicitly.
- **One trailing space split the engines, for over a year.** `dim_duid.DUID` held `'ERB01 '`.
  T-SQL pads strings when comparing (`'ERB01' = 'ERB01 '` is true); DuckDB and Spark do not. The
  same join matched on one engine and dropped rows on the others. Found in the predecessor repo,
  [direct-lake-parquet-layout](https://github.com/djouallah/direct-lake-parquet-layout/blob/main/LEARNINGS.md).
- **Rounding a tie depends on the engine.** `DOUBLE → DECIMAL` is HALF_UP on Spark, HALF_EVEN
  on DuckDB, and a third rule in T-SQL. Over 26.8M rows two correct engines differed by 1e-8.
  That is why `parity.py` gives money columns a 1e-7 relative tolerance and exact-matches
  everything else — and why the tolerance is never raised to make a diff go away.

## Same SQL, different input

- **"Take the next N files" had no `ORDER BY`.** The three DuckDB pre-hooks picked files in no
  order, while Spark and T-SQL took them oldest first. With a backlog bigger than
  `process_limit`, the engines folded different files, and parity reported a logic difference
  that was really an input difference. Now newest first everywhere, pinned by
  `tests_py/test_process_order.py`.
- **One variable meant two things.** `FILES_PATH` was both where the CSVs were downloaded and
  where an engine read them. Re-pointing it for two engines gave those two their own private
  download, so parity was grading engines on different data. Now `LANDING_PATH` and `FILES_PATH`
  are separate.
- **Three generations of `fct_summary` shipped at once** — one per engine family, each
  reasonable. Parity split by up to 63k rows. Now one design on all five.

## Green, but nothing happened

- **dbt builds nothing and exits 0.** Rename a target so it stops matching its folder, and every
  model is disabled; `dbt build` says "Nothing to do" and the run is green.
  `.github/scripts/check_gating.py` exists only to catch this.
- **Spark rebuilds every incremental model from scratch on a fresh runner.** Its metastore is
  empty, so `is_incremental()` is false and every run silently starts over: right numbers, no
  incremental work. `spark_session.py` registers the existing tables before dbt starts.
- **A config the adapter ignores is not an error.** dbt-polars drops `merge_clauses`, so an
  insert-only merge (`do_nothing` on match) quietly *updated* the row
  ([dbt-polars#32](https://github.com/datamindedbe/incubator-dbt-polars/issues/32)).
  dbt-duckdb drops a boolean `false` attach option the same way. A misspelled dbt-polars
  `read_options` key vanishes ([#34](https://github.com/datamindedbe/incubator-dbt-polars/issues/34)),
  and an `on-run-end` hook reports OK without running ([#36](https://github.com/datamindedbe/incubator-dbt-polars/issues/36)).
- **A green run in Fabric does not prove the credentials.** A notebook's own identity already
  reads OneLake, so a profile with no secret at all built every model there — and died on the
  first model on a GitHub runner.

## A check that passes can still be the wrong check

- **Polars read the CSVs 25 columns wide, and the smoke test passed.** `infer_schema => false`
  takes the width from the first line, and a DUNIT record has 53 fields. The probe counted DUNIT
  rows, which fit in the first 25 columns, so it matched. The pipeline then failed on
  `fct_price` (run 38650577769). Now the probe grades the 53rd field.
- **A probe that filters on `column_1` found nothing.** A headerless Polars 2 read numbers its
  columns from `column_0`. An empty result read like "this feature doesn't work" until the
  column names were printed.

## So: validate the output

What actually caught every one of these, in this repo:

1. **A fingerprint per engine** (`macros/parity_fingerprint.sql`): row counts, distinct units,
   days, date range and money sums from the gold layer, taken through each engine's own adapter.
2. **`parity.py` compares them on every run**: counts exact, money within 1e-7. A difference is a
   finding, not a threshold to tune.
3. **Read the result back.** The smoke probes re-read every table they write and grade it against
   a reference read; "dbt said OK" is not accepted as "it worked".
4. **Check the shape, not only the count**: widths, ranges, the last field — counts hide
   truncation.
5. **Make "nothing ran" fail**: `check_gating.py` turns a green no-op into a red run.

Write the SQL however you like, and let AI port it. Then compare the outputs.
