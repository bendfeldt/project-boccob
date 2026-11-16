{{
  config(
    materialized='dimension',
    business_key='case_type_code',
    scd_type=1,
    surrogate_key='case_type_key'
  )
}}
-- Case type dimension, SCD1 (attributes overwritten). Reads the clean layer directly:
-- reference data needs no consolidation, so the optional consolidated layer is skipped.
select
  case_type_code,
  case_type_name,
  department,
  is_active
from {{ ref('case_types') }}
