-- consolidated.party.person was an incremental table: a renamed copy of clean.borgerportal.citizens.
-- It becomes a view (models/consolidated/party/person.sql). Nothing is lost: every row is derived
-- from clean.borgerportal.citizens, which keeps the full change history. The table is dropped here,
-- before dbt creates the view; Polaris drops it with purge (its files stay as noncurrent object
-- versions for the deletion safety window, then the bucket lifecycle / maintenance removes them).
drop table if exists ${consolidated}.party.person;
