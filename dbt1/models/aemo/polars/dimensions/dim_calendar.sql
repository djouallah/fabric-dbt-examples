{#-- Same calendar as every engine: one row per day, 2018-04-01 .. 2026-12-31.
     dbt.date_spine where the DuckDB legs use generate_series, which Polars SQL does not have
     (polars_smoke); its end bound is exclusive, hence 2027-01-01. Insert-only by `append`
     plus the NOT IN filter, which is what the other engines' delete+insert on `date` amounts
     to when the filter already drops every existing date. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

{#-- The spine as a CTE, never a subquery: it is a WITH of its own, and Polars SQL resolves a
     nested WITH inside a CTE but not inside FROM ( ... ) ("relation 'filtered' was not
     found"). --#}
WITH spine AS (
  {{ dbt.date_spine('day', "CAST('2018-04-01' AS DATE)", "CAST('2027-01-01' AS DATE)") }}
)

SELECT
  CAST(date_day AS DATE) AS date,
  CAST(EXTRACT(year FROM date_day) AS INT) AS year,
  CAST(EXTRACT(month FROM date_day) AS INT) AS month
FROM spine
{% if is_incremental() %}
WHERE CAST(date_day AS DATE) NOT IN (SELECT date FROM {{ this }})
{% endif %}
