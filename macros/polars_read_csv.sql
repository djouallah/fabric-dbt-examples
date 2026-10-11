{#-- The polars engine's counterpart of duckdb_read_csv.sql: how its fact models read the
     landed AEMO CSVs. Same spec (aemo_columns.sql), same record rule, same file choice --
     only the mechanics differ, because dbt-polars has neither a read_csv() with options nor
     SQL hooks:

       * THE READ is a dbt source per landing folder (models/aemo/polars/_sources.yml) whose
         read_options pad every row to 131 String columns named column_1..column_131 -- the
         widest AEMO record (DREGION) plus one. A headerless Polars read otherwise takes its
         width from the FIRST line and truncate_ragged_lines cuts every wider record down to
         it: a DUNIT row came back 25 fields wide instead of 53 (polars_smoke run 38121051083).
         The padded `schema` needs dtype NAMES resolved by the adapter -- the fork's
         fix/read-options-dtype-names; see requirements/polars.txt.
       * THE FILE CHOICE happens at parse time with run_query, where the DuckDB legs use a
         `SET VARIABLE` pre_hook: dbt-polars refuses SQL hooks ("there is no database to run
         them on"). The query is the same one, newest first, and the list is rendered into the
         model as a `filename IN (...)` on the folder's glob source. --#}


{#-- column_<i> AS <spec name>, in file order: the spec binds BY POSITION, as on every
     engine (aemo_columns.sql: "Column ORDER is load-bearing"). --#}
{% macro polars_csv_columns(record) %}
  {%- for name in aemo_columns(record) %}
    column_{{ loop.index }} AS {{ name }}{{ "," if not loop.last }}
  {%- endfor %}
{% endmacro %}


{#-- The record-selection predicate from aemo_spec's `equals`/`nonzero`, as in
     duckdb_record_filter. Every column is a String here, so `nonzero` needs an explicit
     cast: comparing a string to a number is how the spark leg once lost 12-16% of its
     intraday SCADA rows (see AGENTS.md). --#}
{% macro polars_record_filter(record) %}
  {%- set spec = aemo_spec(record) -%}
  {%- set parts = [] -%}
  {%- for col, val in spec['equals'] %}{% do parts.append(col ~ " = '" ~ val ~ "'") %}{% endfor -%}
  {%- if spec['nonzero'] %}{% do parts.append('CAST(' ~ spec['nonzero'] ~ ' AS DOUBLE) <> 0') %}{% endif -%}
  {{ parts | join(' AND ') }}
{% endmacro %}


{#-- The unprocessed files of one source_type, as base names, NEWEST FIRST and capped at
     process_limit: the DuckDB pre-hooks' query, run at parse time. `none` outside
     run/build/retry, which the caller treats as "there is work" (no filter), the same safe
     default as every engine's guard. --#}
{% macro polars_pending_files(source_type) %}
  {%- set q -%}
    SELECT DISTINCT archive_path FROM {{ ref('stg_csv_archive_log') }}
    WHERE source_type = '{{ source_type }}'
    {%- if is_incremental() %} AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %}
    ORDER BY archive_path DESC LIMIT {{ env_var('process_limit', '1000') }}
  {%- endset -%}
  {%- if execute and flags.WHICH in ('run', 'build', 'retry') -%}
    {%- set files = [] -%}
    {%- for row in run_query(q).rows -%}{% do files.append(row[0].split('/')[-1]) %}{%- endfor -%}
    {{ return(files) }}
  {%- else -%}
    {{ return(none) }}
  {%- endif -%}
{% endmacro %}


{#-- The staging read for one record: the padded glob source, narrowed to `files`, columns
     named from the spec, filtered to the record. --#}
{% macro polars_staging(record, source_name, files) %}
  SELECT * FROM (
    SELECT
      {{- polars_csv_columns(record) }},
      filename
    FROM {{ source(source_name, 'csv') }}
    {%- if files is not none %}
    WHERE split_part(filename, '/', -1) IN ('{{ files | join("', '") }}')
    {%- endif %}
  ) AS r
  WHERE {{ polars_record_filter(record) }}
{% endmacro %}
