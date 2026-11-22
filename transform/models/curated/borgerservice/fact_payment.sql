{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='payment_id',
    on_schema_change='append_new_columns',
    pre_hook=[
      "{{ keystore_register_inferred('dim_citizen', '(select citizen_id from ' ~ ref('payments') ~ raw_incremental_filter('_source_loaded_at') ~ ')', 'citizen_id') }}",
      "{{ keystore_register_inferred('dim_case_type', '(select case_type_code from ' ~ ref('payments') ~ raw_incremental_filter('_source_loaded_at') ~ ')', 'case_type_code') }}"
    ],
    post_hook=[
      "{{ keystore_mark_used('dim_citizen', 'citizen_key') }}",
      "{{ keystore_mark_used('dim_case_type', 'case_type_key') }}"
    ]
  )
}}
-- Payment fact, grain: one row per payment. Surrogate keys come from the keystore backed
-- dimensions; the citizen version is the one valid on the payment date (late facts get the
-- historically correct version), unknown business keys map to -1, members without master data
-- yet get an inferred key (pre-hooks, over the new rows only).
-- Incremental runs read the new clean rows (pushdown on _loaded_at) plus, as a separate branch,
-- rows whose keys must change: keyed to a member adopted since, or loaded before their case
-- (unknown case type) whose clean row now has the case type.
with batch as (
  select * from {{ ref('payments') }}
  {{ raw_incremental_filter('_source_loaded_at') }}
  {% if is_incremental() %}
  union
  select p.* from {{ ref('payments') }} p
  where p.payment_id in (
    {{ keystore_rekey_adopted('dim_citizen', 'citizen_key', 'payment_id') }}
    union
    select f.payment_id from {{ this }} f
    join {{ ref('payments') }} c on c.payment_id = f.payment_id
    where f.case_type_key = -1 and c.case_type_code is not null
  )
  {% endif %}
)

select
  p.payment_id,
  {{ dim_key('dim_citizen', 'ci') }} as citizen_key,
  {{ dim_key('dim_case_type', 'ct') }} as case_type_key,
  cast(date_format(p.payment_date, '%Y%m%d') as integer) as payment_date_key,
  p.case_id,
  p.amount,
  p.currency_code,
  p.status,
  p.booked_at,
  p._loaded_at as _source_loaded_at,
  {{ audit_columns() }}
from batch p
{{ dim_join('dim_citizen', 'ci', 'p.citizen_id', at='p.payment_date') }}
{{ dim_join('dim_case_type', 'ct', 'p.case_type_code') }}
