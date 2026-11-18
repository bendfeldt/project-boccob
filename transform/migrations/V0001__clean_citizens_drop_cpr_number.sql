-- Release 0.2 - data minimisation (audit L5): clean.borgerportal.citizens no longer carries the
-- CPR number; the pseudonymised citizen_id is the only person identifier from clean onward.
-- Lakes built by release 0.1 still have the column (with values); dbt would keep it under
-- on_schema_change='append_new_columns' and the drift check would stop the run.
-- Idempotent: nothing happens on a lake without the table or without the column.
alter table if exists ${clean}.borgerportal.citizens drop column if exists cpr_number;
-- Dropping a column only hides it: the old data files still contain the values. Rewrite the files
-- and expire the history (time travel of this table is lost - that is the point). Noncurrent object
-- versions of the replaced files are removed by the maintenance job / bucket lifecycle.
-- @purge-history clean.borgerportal.citizens
