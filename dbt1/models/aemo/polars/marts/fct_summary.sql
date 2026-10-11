-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

-- Determinism contract: same inputs => same summary, on every engine, regardless of that
-- engine's run history. Every run emits the COMPLETE recomputation -- the same SQL as a full
-- refresh -- for exactly the dates whose stored content could still be stale, and the write
-- reconciles that batch key by key. The design, the rebuild window and dispatch_duids are
-- the iceberg copy's, verbatim in meaning; read that header for why each piece is there.
--
-- Insert-only, like the DuckDB family: a re-emitted key that is already stored is dropped,
-- a missing one is added. Here that is `append` plus the anti-join at the bottom, because
-- dbt-polars ignores merge_clauses -- the do-nothing merge would UPDATE instead.
--
-- Polars SQL dialect, where the DuckDB copy's does not parse: explicit GROUP BY columns for
-- GROUP BY ALL, scalar subqueries as one-row CROSS JOINs, an INTERVAL string, and TIMESTAMP
-- where the others compare TIMESTAMPTZ (Polars SQL has no zoned type).
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    schema='mart'
) }}

{# No full-history lever inside the model, deliberately. Reset = drop. #}
{# Closes with `%}`, NOT `-%}`: a right-strip glues WITH onto the comment line above. #}
{%- set scoped = is_incremental() %}

WITH
-- The unit universe the DAILY branch can reproduce; gates the intraday branch. Unbounded on
-- purpose (see the iceberg copy).
dispatch_duids AS (
  SELECT DISTINCT DUID FROM {{ ref('fct_scada') }}
),
scada_cutoff AS (
  SELECT MAX(SETTLEMENTDATE) AS max_ts, MAX(DATE) AS max_date FROM {{ ref('fct_scada') }}
),
today_cutoff AS (
  SELECT MAX(SETTLEMENTDATE) AS max_ts FROM {{ ref('fct_scada_today') }}
),
{% if scoped %}
-- Dates whose stored content could differ from a clean recomputation.
rebuild_dates AS (
  -- Never seen before: archive backfill, or a first build catching up.
  SELECT DISTINCT s.DATE AS date FROM {{ ref('fct_scada') }} s
  WHERE s.INTERVENTION = 0
    AND s.DATE NOT IN (SELECT DISTINCT date FROM {{ this }})
  UNION
  -- Recently settled: the last 6 days of the daily feed.
  SELECT DISTINCT s.DATE AS date FROM {{ ref('fct_scada') }} s
  CROSS JOIN scada_cutoff c
  WHERE s.DATE >= c.max_date - INTERVAL '6 days'
  UNION
  -- Still in flux: the intraday feed keeps extending these.
  SELECT DISTINCT s.DATE AS date FROM {{ ref('fct_scada_today') }} s
  UNION
  -- Partially written and never completed (280 matches assert_fct_summary_no_partial_dates).
  SELECT date FROM {{ this }} GROUP BY date HAVING COUNT(DISTINCT time) < 280
),
{% endif %}

daily_rows AS (
  SELECT
    s.DATE AS date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) AS time,
    s.DUID,
    s.INITIALMW,
    p.RRP
  FROM {{ ref('fct_scada') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    {% if scoped %}
    AND s.DATE IN (SELECT date FROM rebuild_dates)
    {% endif %}
),

-- Intraday tail: intervals beyond the daily horizon.
today_rows AS (
  SELECT
    s.DATE AS date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) AS time,
    s.DUID,
    s.INITIALMW,
    p.RRP
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  CROSS JOIN scada_cutoff c
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    -- Only units the daily branch will be able to reproduce once this date settles.
    AND s.DUID IN (SELECT DUID FROM dispatch_duids)
    AND s.SETTLEMENTDATE > c.max_ts
),

daily_summary AS (
  SELECT date, time, DUID, MAX(INITIALMW) AS mw, MAX(RRP) AS price
  FROM daily_rows
  GROUP BY date, time, DUID
  UNION ALL
  SELECT date, time, DUID, MAX(INITIALMW) AS mw, MAX(RRP) AS price
  FROM today_rows
  GROUP BY date, time, DUID
),

batch AS (
  SELECT
    ds.date,
    ds.time,
    ds.DUID,
    CAST(ds.mw AS DECIMAL(18, 4)) AS mw,
    CAST(ds.price AS DECIMAL(18, 4)) AS price,
    -- Provenance column only (see the iceberg copy).
    GREATEST(sc.max_ts, COALESCE(tc.max_ts, CAST('1900-01-01 00:00:00' AS TIMESTAMP))) AS cutoff
  FROM daily_summary ds
  CROSS JOIN scada_cutoff sc
  CROSS JOIN today_cutoff tc
)

SELECT b.date, b.time, b.DUID, b.mw, b.price, b.cutoff
FROM batch b
{% if scoped %}
-- Insert-only: a key already stored is left alone (the DuckDB legs' WHEN MATCHED DO NOTHING).
LEFT JOIN (SELECT date, time, DUID, 1 AS stored FROM {{ this }}) t
  ON b.date = t.date AND b.time = t.time AND b.DUID = t.DUID
WHERE t.stored IS NULL
{% endif %}
ORDER BY b.date, b.time
