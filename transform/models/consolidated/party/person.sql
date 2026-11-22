{{
  config(
    materialized='view',
    view_security='invoker',
    persist_docs={'relation': false, 'columns': false}
  )
}}
-- Business entity PERSON: one row per known state of a person, harmonised across source systems.
-- A view while one source feeds it: no extra copy of personal data to erase, retain or purge
-- (clean.borgerportal.citizens keeps the history). Further sources are added as extra UNION ALL
-- branches that map their columns to the business vocabulary below (the pseudonymised person_id
-- is the same for the same CPR number in every source, which is what makes consolidation
-- possible); materialize it once a second source makes the union expensive.
-- security invoker: readers need read access to the clean tables themselves (OPA).
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
