{{ config(materialized='table') }}
-- Calendar dimension. The key is the ISO date as integer (yyyymmdd) - deterministic by
-- definition, therefore not managed by the keystore.
select
  cast(date_format(d, '%Y%m%d') as integer) as date_key,
  cast(d as date) as calendar_date,
  year(d) as calendar_year,
  quarter(d) as calendar_quarter,
  month(d) as calendar_month,
  date_format(d, '%Y-%m') as year_month,
  week(d) as iso_week,
  day_of_week(d) as iso_day_of_week,
  day_of_week(d) in (6, 7) as is_weekend
from unnest(sequence(date '1990-01-01', date '2099-12-31', interval '1' day)) as t (d)  -- history and future payments
