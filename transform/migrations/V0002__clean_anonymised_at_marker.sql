-- deregistration erasure keeps bookkeeping rows anonymised until their own retention
-- expires. clean.borgerportal.cases and payments get the marker column
-- _anonymised_at. dbt would add it by itself (append_new_columns), but the erasure job may run
-- before the first dbt build of the release and its SQL already sets the column.
alter table if exists ${clean}.borgerportal.cases add column if not exists _anonymised_at timestamp(6) with time zone;
alter table if exists ${clean}.borgerportal.payments add column if not exists _anonymised_at timestamp(6) with time zone;
