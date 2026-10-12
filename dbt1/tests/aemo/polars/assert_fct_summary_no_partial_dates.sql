-- Crater tripwire: a date that lands PARTIALLY in the summary and is then never completed.
-- Mechanism: during backfill a date enters the summary with only the intervals fct_scada
-- happened to hold at that moment (typically the 49 from the previous day's archive file);
-- the daily branch used to skip any date already present, so the rest of the day never
-- arrived. fct_summary now reprocesses dates it holds partially, which heals these -- this
-- test is the guard that it keeps working. Observed 2025-09-13: 49 in the summary, 288 in
-- fct_scada.
--
-- Compares against fct_scada, NOT against a fixed 288, because "short" and "wrong" are not
-- the same thing. A date can be short at the SOURCE: its first ~4h live in the PREVIOUS
-- day's archive file, so until the backfill loads that file the date sits legitimately
-- capped. fct_scada agrees with the summary on those, and flagging them is noise.
-- Do not re-derive this as "the previous day is missing, so the cap is permanent" -- it
-- usually is not; comparing against the source is what makes the distinction
-- self-correcting.
--
-- The 280 pre-filter is a cheap shortlist off fct_summary alone, not the assertion; only
-- those few dates are probed against fct_scada (measured: ~46s for 2 dates vs ~233s for a
-- full-year aggregate). The latest date is excluded -- it is legitimately still filling.

--
-- Polars SQL dialect: the correlated per-date count is a GROUP BY joined back, the
-- "latest date" a one-row CROSS JOIN, and the 12-month bound an INTERVAL string.

WITH per_date AS (
  SELECT date, COUNT(DISTINCT time) AS intervals
  FROM {{ ref('fct_summary') }}
  -- dbt.current_timestamp(): Polars SQL has no current_date; dbt-polars renders the run's
  -- start as a literal.
  WHERE date >= CAST({{ dbt.current_timestamp() }} AS DATE) - INTERVAL '12 months'
  GROUP BY date
),
latest AS (
  SELECT MAX(date) AS max_date FROM per_date
),
short AS (
  SELECT p.date, p.intervals FROM per_date p
  CROSS JOIN latest l
  WHERE p.date < l.max_date AND p.intervals < 280
),
scada_per_date AS (
  SELECT sc.DATE AS date, COUNT(DISTINCT sc.SETTLEMENTDATE) AS scada_intervals
  FROM {{ ref('fct_scada') }} sc
  WHERE sc.INTERVENTION = 0 AND sc.INITIALMW <> 0
    AND sc.DATE IN (SELECT date FROM short)
  GROUP BY sc.DATE
)
SELECT s.date, s.intervals AS summary_intervals, c.scada_intervals
FROM short s
JOIN scada_per_date c ON s.date = c.date
WHERE s.intervals < c.scada_intervals
ORDER BY s.date
