{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='case_id',
    on_schema_change='append_new_columns'
  )
}}
-- Latest known state of every case.
with raw as (
  select {{ raw_columns() }}
  from {{ source('raw_borgerportal', 'cases') }}
  {{ raw_incremental_filter() }}
),

parsed as (
  select
    {{ json_text('$["@id"]') }} as case_id,
    {{ subject_key('cpr') }} as citizen_id,
    {{ json_text('$.type["@code"]') }} as case_type_code,
    lower({{ json_text('$.status') }}) as status,
    try_cast({{ json_text('$.opened') }} as date) as opened_date,
    try_cast({{ json_text('$.closed') }} as date) as closed_date,
    {{ json_text('$.caseworker') }} as caseworker_initials,
    {{ ts6("try(from_iso8601_timestamp(" ~ json_text('$.updated_at') ~ "))") }} as source_updated_at,
    _raw_file,
    _raw_row,
    _load_id,
    _loaded_at
  from raw
),

deduplicated as (
  select
    *,
    row_number() over (
      partition by case_id
      order by source_updated_at desc, _load_id desc, _raw_row desc
    ) as _rn
  from parsed
  where case_id is not null
)

select
  case_id,
  citizen_id,
  case_type_code,
  status,
  opened_date,
  closed_date,
  caseworker_initials,
  source_updated_at,
  _raw_file,
  _raw_row,
  _load_id,
  _loaded_at,
  {{ ts6('null') }} as _anonymised_at,  -- set by a deregistration erasure (docs/gdpr.md)
  {{ audit_columns() }}
from deduplicated d
where _rn = 1
{% if is_incremental() %}
  -- an older state (e.g. a re-delivered or re-indexed load) never overwrites a newer one
  and not exists (select 1 from {{ this }} t where t.case_id = d.case_id and t.source_updated_at > d.source_updated_at)
{% endif %}
