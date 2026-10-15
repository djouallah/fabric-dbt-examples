{#-- The polars engine's counterpart of duckdb_read_csv.sql: how its fact models read the
     landed AEMO CSVs. Same spec (aemo_columns.sql), same record rule, same file choice --
     only the mechanics differ:

       * THE READ is Polars SQL read_csv(), ONE PER PICKED FILE, joined by UNION ALL, with no
         header and `infer_schema_length => NULL` (pola-rs/polars#29917): the width is the
         WIDEST row's. Not `infer_schema => false`, which reads only the first line's width --
         a DUNIT row came back 25 fields wide instead of 53 (pipeline run 38650577769) -- and
         not over the folder's glob, where the whole-file inference ran past 20 minutes before
         the filename filter applied (polars_smoke run 38653690208). Types are inferred, so
         every spec column is cast back to the String the DuckDB legs read (all_varchar).
         Columns are numbered from column_0. The credential is AZURE_STORAGE_TOKEN, which
         run_dbt.py sets: read_csv() takes none.
       * THE FILE CHOICE happens at parse time with run_query, where the DuckDB legs use a
         `SET VARIABLE` pre_hook: dbt-polars refuses SQL hooks ("there is no database to run
         them on"). The query is the same one, newest first, and each file it lists becomes
         one read_csv() in the model. --#}


{#-- column_<i> AS <spec name>, in file order from column_0: the spec binds BY POSITION, as on
     every engine (aemo_columns.sql: "Column ORDER is load-bearing"). Cast to VARCHAR because
     read_csv() infers types; the models cast from String, as on the DuckDB legs. --#}
{% macro polars_csv_columns(record) %}
  {%- for name in aemo_columns(record) %}
    CAST(column_{{ loop.index0 }} AS VARCHAR) AS {{ name }}{{ "," if not loop.last }}
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


{#-- The staging read for one record: one read_csv() per file in `files`, columns named from
     the spec, filtered to the record. `files` is none outside run/build/retry, where nothing
     executes: the folder's glob then stands in, so the model still compiles. --#}
{% macro polars_staging(record, folder, files) %}
  {%- set base = get_csv_archive_path() ~ '/' ~ folder ~ '/' -%}
  {%- set paths = [base ~ '*.CSV'] if files is none else (files | map('string') | list) -%}
  SELECT * FROM (
    {%- for f in paths %}
    SELECT
      {{- polars_csv_columns(record) }},
      filename
    FROM read_csv('{{ f if files is none else base ~ f }}',
      has_header => false, skip_rows => 1, infer_schema_length => NULL,
      try_parse_dates => false, include_file_paths => 'filename')
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
    {%- endfor %}
  ) AS r
  WHERE {{ polars_record_filter(record) }}
{% endmacro %}
