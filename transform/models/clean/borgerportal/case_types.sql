{{ config(materialized='table') }}
-- Reference data: the newest delivered snapshot (small, rebuilt every run).
-- "Newest" is decided by the load id (a numeric timestamp string, unique per load), not by
-- _loaded_at, which is shared by all loads indexed in the same run (catch-up / backfill).
-- The entity is declared `snapshot: true`, so raw index retention always keeps its latest load.
with raw as (
  select {{ raw_columns() }}
  from {{ source('raw_borgerportal', 'case_types') }}
),

latest as (
  select * from raw
  where _load_id = (select max_by(_load_id, cast(_load_id as double)) from raw)
)

select distinct
  {{ json_text('$.code') }} as case_type_code,
  {{ json_text('$.name') }} as case_type_name,
  {{ json_text('$.department') }} as department,
  coalesce(try_cast({{ json_text('$.active') }} as boolean), true) as is_active,
  _load_id,
  _loaded_at,
  {{ audit_columns() }}
from latest
