{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='payment_id',
    on_schema_change='append_new_columns',
    pre_hook=[
      "{{ keystore_register_inferred('dim_citizen', ref('payments'), 'citizen_id') }}",
      "{{ keystore_register_inferred('dim_case_type', ref('payments'), 'case_type_code') }}"
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
-- yet get an inferred key (pre-hook); rows keyed to a member that has since been adopted are
-- re-keyed to the version valid at their payment date.
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
from {{ ref('payments') }} p
{{ dim_join('dim_citizen', 'ci', 'p.citizen_id', at='p.payment_date') }}
{{ dim_join('dim_case_type', 'ct', 'p.case_type_code') }}
{% if is_incremental() %}
where p._loaded_at > (select coalesce(max(_source_loaded_at), timestamp '1900-01-01 00:00:00 UTC') from {{ this }})
  {{ keystore_rekey_adopted('dim_citizen', 'citizen_key', 'p.payment_id', 'payment_id') }}
{% endif %}
