{#-- AEMO DUNIT record from the daily archive files: all 53 source columns.

     Insert-only, as on every engine, but by `append`: dbt-polars ignores merge_clauses, so a
     do-nothing merge would UPDATE matched rows instead (polars_smoke probe 4). The unique_key
     every engine dedupes on starts with `file`, and polars_pending_files() excludes every
     file already in {{ this }}, so appending only unprocessed files IS the do-nothing merge.

     The files are chosen at parse time -- dbt-polars has no SQL hooks -- by the same query
     the DuckDB pre-hooks run, NEWEST FIRST, capped at process_limit. See
     macros/polars_read_csv.sql. --#}
{%- set spec = aemo_spec('scada') -%}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

-- depends_on: {{ ref('stg_csv_archive_log') }}
{%- set files = polars_pending_files(spec['source_type']) %}

{% if files is none or files | length > 0 %}
WITH staging AS (
  {{ polars_staging('scada', 'polars_daily', files) }}
)

SELECT
  UNIT,
  DUID,
  {%- for name in aemo_cast_columns('scada') %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('filename') }} AS file,
  {#-- TIMESTAMP, not TIMESTAMPTZ: Polars SQL has no zoned timestamp by any spelling
       (polars_smoke probes 20-22). The values are the same AEMO market times. --#}
  CAST(SETTLEMENTDATE AS TIMESTAMP) AS SETTLEMENTDATE,
  CAST(CAST(SETTLEMENTDATE AS TIMESTAMP) AS DATE) AS DATE,
  CAST(EXTRACT(year FROM CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS YEAR,
  {#-- Monthly partition key (YYYYMM), emitted on both daily facts by every engine. --#}
  CAST(EXTRACT(year FROM CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) * 100
    + CAST(EXTRACT(month FROM CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS month_key
FROM staging
{% else %}
-- No unprocessed files: an empty result keeps the existing data untouched.
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
