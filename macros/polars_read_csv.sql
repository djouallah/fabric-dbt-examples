{#-- The polars engine's counterpart of duckdb_read_csv.sql: how its fact models read the
     landed AEMO CSVs. Same spec (aemo_columns.sql), same record rule, same file choice --
     only the mechanics differ:

       * THE READ is ONE Polars SQL read_csv() over a glob of the run's files, with no header and a
         FIXED schema: the record's own columns, every one VARCHAR and named from the spec,
         with `missing_columns => 'insert'` padding a shorter row and `truncate_ragged_lines`
         cutting a wider one (another record type, which the WHERE drops anyway) -- the DuckDB
         legs' all_varchar + null_padding + explicit `columns`. No inference: the widest-row
         inference it replaces (`infer_schema_length => NULL`, pola-rs/polars#29917) read every
         file twice. `missing_columns` is in SQL only since pola-rs/polars#29975 (#29970), hence
         the Polars main pin. The credential is AZURE_STORAGE_TOKEN, which run_dbt.py sets:
         read_csv() takes none.
       * THE FILE CHOICE happens at parse time with run_query, where the DuckDB legs use a
         `SET VARIABLE` pre_hook: dbt-polars refuses SQL hooks ("there is no database to run
         them on"). The query is the same one, newest first, and the files it lists become the
         glob itself, `<folder>/*(<file>|<file>|...)`, plus a filter on `filename`.

     WHY THAT GLOB WORKS. SQL read_csv() takes no list of paths (pola-rs/polars#29969), and a
     plain `*.CSV` glob reads EVERY file in the folder: Polars skips no file on a filename
     filter (polars_smoke runs 38733171553, 38735481756), and pipeline run 38907253166 sat on the
     whole landing folder until it was cancelled. But a CLOUD glob is a regex in disguise:
     crates/polars-io/src/cloud/glob.rs (extract_prefix_expansion) rewrites only `*` and `.` and
     passes every other character through, so `*(a.CSV|b.CSV)` matches exactly those names -- the
     list, spelled as an alternation (Spark's `{a,b}` would not work). The folder is listed once
     and only the matches are read. That is undocumented, so the `filename` filter stays as the
     guard: if the glob ever matched more, the result would still be exact, only slower. The
     UNION ALL of one read_csv() per file is not coming back. When #29969 lands, the alternation
     becomes a list and nothing else changes. --#}


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


{#-- The staging read for one record: one read_csv() over a glob that names the run's `files`,
     the spec's columns bound BY POSITION (aemo_columns.sql: "Column ORDER is load-bearing"),
     narrowed to the record. `files` is none outside run/build/retry, where nothing executes: the
     model then compiles over `*.CSV` with no file filter. --#}
{% macro polars_staging(record, folder, files) %}
  {%- set base = get_csv_archive_path() ~ '/' ~ folder ~ '/' -%}
  {%- set glob = base ~ ('*.CSV' if files is none else '*(' ~ (files | join('|')) ~ ')') -%}
  SELECT * FROM read_csv('{{ glob }}',
    has_header => false, skip_rows => 1,
    schema => { {%- for name in aemo_columns(record) %}'{{ name }}': 'VARCHAR'{{ ", " if not loop.last }}{% endfor %} },
    missing_columns => 'insert', truncate_ragged_lines => true,
    include_file_paths => 'filename')
  WHERE {{ polars_record_filter(record) }}
  {%- if files is not none %}
    AND split_part(filename, '/', -1) IN ({% for f in files %}'{{ f }}'{{ ", " if not loop.last }}{% endfor %})
  {%- endif %}
{% endmacro %}
