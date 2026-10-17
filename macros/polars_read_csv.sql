{#-- The polars engine's counterpart of duckdb_read_csv.sql: how its fact models read the
     landed AEMO CSVs. Same spec (aemo_columns.sql), same record rule, same file choice --
     only the mechanics differ:

       * THE READ is Polars SQL read_csv(), ONE PER PICKED FILE, joined by UNION ALL, with no
         header and a FIXED schema: the record's own columns, every one VARCHAR and named from
         the spec, with `missing_columns => 'insert'` padding a shorter row and
         `truncate_ragged_lines` cutting a wider one (another record type, which the WHERE drops
         anyway) -- the DuckDB legs' all_varchar + null_padding + explicit `columns`. No
         inference, one pass per file: polars_smoke run 38904749014 read 10 files in 5 s this
         way, against 42 s for the widest-row inference (`infer_schema_length => NULL`,
         pola-rs/polars#29917) it replaces. `missing_columns` is in SQL only since
         pola-rs/polars#29975 (#29970), hence the Polars main pin. The credential is
         AZURE_STORAGE_TOKEN, which run_dbt.py sets: read_csv() takes none.
       * WHY ONE read_csv() PER FILE. SQL read_csv() takes no list of paths
         (pola-rs/polars#29969), and a glob over the folder reads EVERY file in it: Polars
         skips no file on a filename filter (polars_smoke runs 38733171553, 38735481756;
         pipeline run 38907253166 sat on the whole landing folder until it was cancelled). When
         #29969 lands, the UNION ALL becomes one read_csv() of the list.
       * THE FILE CHOICE happens at parse time with run_query, where the DuckDB legs use a
         `SET VARIABLE` pre_hook: dbt-polars refuses SQL hooks ("there is no database to run
         them on"). The query is the same one, newest first, and each file it lists becomes
         one read_csv() in the model. --#}


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


{#-- The staging read for one record: one read_csv() per file in `files`, the spec's columns
     bound BY POSITION (aemo_columns.sql: "Column ORDER is load-bearing"), filtered to the
     record. `files` is none outside run/build/retry, where nothing executes: the folder's glob
     then stands in, so the model still compiles. --#}
{% macro polars_staging(record, folder, files) %}
  {%- set base = get_csv_archive_path() ~ '/' ~ folder ~ '/' -%}
  {%- set paths = [base ~ '*.CSV'] if files is none else (files | map('string') | list) -%}
  {#- THIS UNION DOES NOT FIT 50 FILES ON THE 7 GB RUNNER, and nothing in this macro can make
      it. Polars plans SQL UNION ALL as an UNORDERED union, and its streaming engine runs every
      input of one at once: peak RSS grows ~0.34 GB per file while pl.concat (the ORDERED union
      of the same scans) stays flat -- pola-rs/polars#30029. polars_smoke run 38998855350: OOM
      at 25 files whether streaming, in-memory or on two threads, local disk or OneLake; one
      Python scan_csv over the list held 50 in 4.1 GB. SQL has neither the list (#29969) nor
      the ordered union, so fct_price at process_limit 50 dies until one of the two lands.
      The record filter sits in every branch; moving it there (de8e268) did not help, because
      Polars pushes it into every scan either way. #}
  {%- for f in paths %}
  SELECT * FROM read_csv('{{ f if files is none else base ~ f }}',
    has_header => false, skip_rows => 1,
    schema => { {%- for name in aemo_columns(record) %}'{{ name }}': 'VARCHAR'{{ ", " if not loop.last }}{% endfor %} },
    missing_columns => 'insert', truncate_ragged_lines => true,
    include_file_paths => 'filename')
  WHERE {{ polars_record_filter(record) }}
  {%- if not loop.last %}
  UNION ALL
  {%- endif %}
  {%- endfor %}
{% endmacro %}
