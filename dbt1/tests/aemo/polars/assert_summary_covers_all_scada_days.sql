{{ config(severity='warn') }}
-- The summary should hold at least as many distinct days as fct_scada.
-- A WARNING, not a failure, and for the same reason as the assert_all_*_files_processed_*
-- tests: with a small process_limit a date whose ingested files are all INTERVENTION=1 or carry
-- unmapped DUIDs produces zero summary rows and trips this until the backlog has converged.
-- Investigate a gap that STOPS closing, not a gap that exists.
-- Polars SQL dialect: the two scalar subqueries as a one-row CROSS JOIN.
SELECT a.scada_days, b.summary_days
FROM (SELECT COUNT(DISTINCT DATE) AS scada_days FROM {{ ref('fct_scada') }} WHERE INTERVENTION = 0) a
CROSS JOIN (SELECT COUNT(DISTINCT date) AS summary_days FROM {{ ref('fct_summary') }}) b
WHERE a.scada_days > b.summary_days
