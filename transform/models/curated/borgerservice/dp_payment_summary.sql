{{ config(materialized='table') }}
-- Data product: monthly payment totals per municipality and case type. No person-level data.
select
  d.year_month,
  ci.municipality_code,
  ct.case_type_code,
  ct.case_type_name,
  count(*) as payment_count,
  -- people, not dimension versions: an SCD2 citizen has one key per version
  count(distinct ci.person_id) as citizen_count,
  cast(sum(f.amount) as decimal(38, 2)) as total_amount,
  count(distinct ci.person_id) < {{ var('small_cell_threshold') }} as is_small_cell
from {{ ref('fact_payment') }} f
join {{ ref('dim_date') }} d on d.date_key = f.payment_date_key
join {{ ref('dim_citizen') }} ci on ci.citizen_key = f.citizen_key
join {{ ref('dim_case_type') }} ct on ct.case_type_key = f.case_type_key
group by 1, 2, 3, 4
