{# Helpers for reading the schema-on-read raw index tables. #}

{% macro raw_columns() -%}
  payload, _raw_file, _raw_row, _load_id, _loaded_at, _subject_keys
{%- endmacro %}

{# Only rows indexed after the newest row already present in the incremental model. #}
{% macro raw_incremental_filter(loaded_at_column='_loaded_at') -%}
  {%- if is_incremental() %}
  where _loaded_at > (
    select coalesce(max({{ loaded_at_column }}), timestamp '1900-01-01 00:00:00 UTC') from {{ this }}
  )
  {%- endif %}
{%- endmacro %}

{% macro json_text(path) -%} nullif(trim(json_extract_scalar(payload, '{{ path }}')), '') {%- endmacro %}

{# Pseudonymised person key computed by the index stage: HMAC-SHA256(pepper, identifier).
   The pepper never reaches SQL text, query logs or compiled artefacts. #}
{% macro subject_key(name) -%} json_extract_scalar(_subject_keys, '$.{{ name }}') {%- endmacro %}

{% macro ts6(expression) -%} cast({{ expression }} as timestamp(6) with time zone) {%- endmacro %}

{# '1.234,56' -> decimal #}
{% macro danish_decimal(expression, precision=18, scale=2) -%}
  try_cast(replace(replace({{ expression }}, '.', ''), ',', '.') as decimal({{ precision }}, {{ scale }}))
{%- endmacro %}

{# 'dd-mm-yyyy' -> date #}
{% macro danish_date(expression) -%} try(cast(date_parse({{ expression }}, '%d-%m-%Y') as date)) {%- endmacro %}

{% macro audit_columns() -%}
  cast('{{ invocation_id }}' as varchar) as _invocation_id,
  {{ ts6('current_timestamp') }} as _transformed_at
{%- endmacro %}
