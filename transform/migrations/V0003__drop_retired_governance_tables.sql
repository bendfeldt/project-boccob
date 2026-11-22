-- Tables of earlier designs that nothing reads or writes any more:
-- the advisory lock of the migration runner (migrations are serialised by the Dagster writer
-- pool; the CLI applies only in dev) and the staging table of the erasure audit digest (the
-- digest is inserted directly).
drop table if exists ${curated}.audit.schema_migration_lock;
drop table if exists ${raw}.governance.erasure_digests;
