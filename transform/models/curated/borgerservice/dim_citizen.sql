{{
  config(
    materialized='dimension',
    business_key='person_id',
    effective_from='valid_from',
    surrogate_key='citizen_key'
  )
}}
-- Citizen dimension, SCD2 (default). Built from the consolidated business entity Person.
select
  person_id,
  valid_from,
  full_name,
  postal_code,
  postal_district,
  municipality_code,
  email,
  year(birth_date) as birth_year,
  is_deregistered
from {{ ref('person') }}
