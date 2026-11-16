{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='payment_id',
    on_schema_change='fail'
  )
}}
-- Payment transactions, typed (Danish number/date formats) and denormalised with case attributes.
with raw as (
  select {{ raw_columns() }}
  from {{ source('raw_borgerportal', 'payments') }}
  {{ raw_incremental_filter() }}
),

parsed as (
  select
    {{ json_text('$.payment_id') }} as payment_id,
    {{ json_text('$.case_id') }} as case_id,
    {{ subject_key('cpr') }} as citizen_id,
    {{ danish_decimal(json_text('$.amount')) }} as amount,
    upper({{ json_text('$.currency') }}) as currency_code,
    {{ danish_date(json_text('$.payment_date')) }} as payment_date,
    {{ ts6("try(cast(" ~ json_text('$.booked_at') ~ " as timestamp(6)))") }} as booked_at,
    lower({{ json_text('$.status') }}) as status,
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
      partition by payment_id
      order by booked_at desc, _load_id desc, _raw_row desc
    ) as _rn
  from parsed
  where payment_id is not null
)

select
  p.payment_id,
  p.case_id,
  p.citizen_id,
  c.case_type_code,
  p.amount,
  p.currency_code,
  p.payment_date,
  p.booked_at,
  p.status,
  p._raw_file,
  p._raw_row,
  p._load_id,
  p._loaded_at,
  {{ audit_columns() }}
from deduplicated p
left join {{ ref('cases') }} c on c.case_id = p.case_id
where p._rn = 1
