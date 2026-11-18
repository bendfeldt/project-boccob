{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['citizen_id', 'source_updated_at'],
    on_schema_change='fail'
  )
}}
-- One row per delivered state of a citizen (change history, source aligned).
-- Parsed + typed + deduplicated; the address is denormalised into the row.
-- Data minimisation: the CPR number is not carried over. The pseudonymised key citizen_id
-- (HMAC of the CPR, computed at indexing) is the only person identifier from clean onward;
-- the CPR itself stays in the raw layer (native files and the masked raw index payload).
with raw as (
  select {{ raw_columns() }}
  from {{ source('raw_borgerportal', 'citizens') }}
  {{ raw_incremental_filter() }}
),

parsed as (
  select
    {{ subject_key('cpr') }} as citizen_id,
    {{ json_text('$.name.first') }} as first_name,
    {{ json_text('$.name.last') }} as last_name,
    {{ json_text('$.address.street') }} as street,
    {{ json_text('$.address.house_no') }} as house_no,
    {{ json_text('$.address.postal_code') }} as postal_code,
    {{ json_text('$.address.city') }} as city,
    {{ json_text('$.address.municipality_code') }} as municipality_code,
    lower({{ json_text('$.contact.email') }}) as email,
    {{ json_text('$.contact.phone') }} as phone,
    try_cast({{ json_text('$.birth_date') }} as date) as birth_date,
    coalesce(try_cast({{ json_text('$.deleted') }} as boolean), false) as is_deleted,
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
      partition by citizen_id, source_updated_at
      order by _load_id desc, _raw_row desc
    ) as _rn
  from parsed
  where citizen_id is not null and source_updated_at is not null
)

select
  citizen_id,
  first_name,
  last_name,
  concat_ws(' ', first_name, last_name) as full_name,
  street,
  house_no,
  concat_ws(' ', street, house_no) as address_line,
  postal_code,
  city,
  municipality_code,
  email,
  phone,
  birth_date,
  is_deleted,
  source_updated_at,
  _raw_file,
  _raw_row,
  _load_id,
  _loaded_at,
  {{ audit_columns() }}
from deduplicated
where _rn = 1
