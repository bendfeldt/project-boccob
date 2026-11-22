{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['person_id', 'valid_from'],
    on_schema_change='append_new_columns'
  )
}}
-- Business entity PERSON: one row per known state of a person, harmonised across source systems.
-- Today only Borgerportal feeds it; further sources are added as extra UNION ALL branches that
-- map their columns to the business vocabulary below (the pseudonymised person_id is the same
-- for the same CPR number in every source, which is what makes consolidation possible).
with borgerportal as (
  select
    citizen_id as person_id,
    'borgerportal' as source_system,
    first_name as given_name,
    last_name as family_name,
    full_name,
    address_line,
    postal_code,
    city as postal_district,
    municipality_code,
    email,
    phone,
    birth_date,
    is_deleted as is_deregistered,
    source_updated_at as valid_from,
    _loaded_at as _source_loaded_at
  from {{ ref('citizens') }}
  {% if is_incremental() %}
  where _loaded_at > (select coalesce(max(_source_loaded_at), timestamp '1900-01-01 00:00:00 UTC') from {{ this }})
  {% endif %}
)

select
  *,
  {{ audit_columns() }}
from borgerportal
