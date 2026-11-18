# Releases and schema evolution

A release of this repository is two artifacts (code location image, OPA bundle, see
[platform-contract.md](platform-contract.md)). Most releases change only code. When a release changes
the shape of a table that keeps history, the lake has to be brought to the new shape **without a
rebuild**, because a rebuild is not possible: the raw layer keeps native files for 30 days and index
rows for 90 days (`governance/retention.yml`), while clean keeps the citizen change history and
5 years of bookkeeping rows, consolidated and curated keep SCD2 versions, and the keystore keeps
every surrogate key ever issued. A rebuild from raw would silently lose all of that; dimension rows
would disappear while the keystore and the facts still refer to their keys.

## Which tables hold history

| Materialization | Tables | Shape change |
|---|---|---|
| `incremental` | `clean.borgerportal.{citizens,cases,payments}`, `consolidated.party.person`, `curated.borgerservice.fact_payment` | additive: automatic; anything else: migration |
| keystore (macros) | `curated.keystore.*` | migration |
| governance (python) | `raw.governance.erasure_requests` | evolves itself (pyiceberg `union_by_name`, nullable columns) |
| `table`, `dimension` | `clean.borgerportal.case_types`, `curated.borgerservice.dim_*`, `dp_*` | rebuilt from their sources on every run - nothing to do (keys come from the keystore) |

## The three mechanisms

1. **Additive changes are automatic.** Every incremental model uses
   `on_schema_change='append_new_columns'`: a new column is added to the existing table by the next
   run; existing rows read `NULL` (backfill, if needed, is a migration). `sync_all_columns` is not
   used anywhere: it would drop a column - with its data - as soon as a model stops selecting it,
   and every incremental model here holds history that cannot be recomputed.
2. **Everything else is a versioned migration.** Dropping, renaming or retyping a column, moving or
   backfilling data, changing a keystore table: a SQL file `transform/migrations/V<nnnn>__<slug>.sql`
   (format in `governance/migrations/runner.py`):
   * statements end with `;` at the end of a line; `${raw}`, `${clean}`, `${consolidated}`,
     `${curated}` are the layer catalogs;
   * every statement is idempotent and tolerates a missing table (`ALTER TABLE IF EXISTS ...
     DROP COLUMN IF EXISTS ...`): on a new lake the migration is recorded without effect, and a
     migration that failed half way simply runs again;
   * `-- @purge-history <layer>.<schema>.<table>` rewrites every row of the table and then expires
     its history (one full rewrite; time travel of the table is lost) - use it when the change
     removes personal data: a dropped column is only hidden by Iceberg until the old files are gone,
     and OPTIMIZE alone leaves a lone file without delete files untouched. The replaced files remain
     as noncurrent object versions until the maintenance job / bucket lifecycle removes them;
   * no key-like or CPR-shaped literal (query text is archived WORM; the runner refuses such files);
   * never edit a migration once it is released: the runner compares checksums and stops.

   Applied migrations are recorded in `curated.audit.schema_migrations` (version, name, checksum,
   statement count, time, principal, duration). The schema `curated.audit` is protected: only the
   pipeline service group `lakehouse-service` writes it (OPA). If the lake knows a migration that the
   deployed release does not (a rollback to an older image), the run stops: roll forward, or restore
   the lake together with the old image.

   After the migrations the runner compares the live columns of every incremental model with the
   model (drift check): a column that exists in the lake but not in the model would otherwise be
   kept, with its data, by `append_new_columns` - the run fails until a migration removes it.
3. **No full refresh outside dev.** With `LAKEHOUSE_ENV` other than `dev` (unset counts as
   production) `dbt ... --full-refresh` of an incremental model is refused before anything runs
   (`on-run-start` hook) and again inside the materialization (`should_full_refresh` override, also
   for a `full_refresh: true` config). The integration harness and local development set
   `LAKEHOUSE_ENV=dev`.

## Where the migrations run

The Dagster asset `curated/audit/schema_migrations` (pool `lakehouse_writer`) applies the pending
migrations. Every dbt model without an upstream model depends on it, so in `daily_refresh` and
`transform` the migrations always run before the first model of a release. The job `migrate` runs
them alone. The `erasure` and `retention` jobs run them as their first step as well, because their
SQL is derived from the release's model metadata and may already refer to a new column.
By hand: `make migrate-status` (read-only) and `make migrate` (`lakehouse-migrate`), only while no
other writer is active.

## Release procedure

1. **Develop.** Change the models; for every non-additive change of a table in the first table above
   add the next `V<nnnn>__*.sql`. Run `make lint test` and `make integration`: the integration test
   builds the lake with the previous model shape, upgrades it with the new release (migrations through
   `daily_refresh`) and checks that keystore keys, SCD2 versions and the clean history survive. Add
   the previous shape of your change to `previous_release_project()` in `tests/integration/test_e2e.py`.
2. **Build and publish** the artifacts (`make image image-push bundle bundle-push`) and hand the
   digest-pinned references to the platform repository.
3. **Before the deployment:** make sure a recent backup of the lake exists (platform: Polaris database,
   bucket versioning / off-site copy). A migration that rewrites data is not reversible otherwise.
4. **Deploy** the new image and bundle (IaC). Let running jobs finish; schedules may stay on - every
   writing job waits for the `lakehouse_writer` pool.
5. **Migrate.** Launch the Dagster job `migrate` (or let the next `daily_refresh` do it) and check the
   materialization metadata (`applied`, `history_purged`) and `curated.audit.schema_migrations`.
   A failure leaves the lake on the old shape (or, for a multi-statement migration, partially
   migrated - idempotent statements make the re-run safe): fix forward with a new release.
6. **Verify.** The next `daily_refresh` must pass all asset checks (keystore integrity, stability
   since baseline, SCD2, usage).

## Break-glass repair of protected schemas

`curated.keystore`, `curated.audit` and `raw.governance` accept writes only from the service group
(`lakehouse-service`, i.e. `svc-dagster`); platform administrators have no bypass (audit M-1). A repair
- e.g. a corrupted keystore row or a stuck erasure request - therefore runs **as a migration**:

1. A ticket describes the defect and the fix; a second person reviews the migration file (four eyes:
   code review of the repository), which must not contain identifiers (the runner refuses key-like
   literals; refer to rows through table data, e.g. the request id of a queued erasure).
2. The fix is released like any other change and applied by the `migrate` job under the service
   account. The change is recorded in `curated.audit.schema_migrations` (who, when, checksum) and the
   statements are in the WORM query archive.

Only when the pipeline itself cannot run (e.g. Dagster is down) may an operator execute the reviewed
statements with the `svc-dagster` credentials directly (a one-off pod from the code location image:
`lakehouse-migrate`), with a second person present and the ticket referenced; the platform must
alert on every interactive use of that service account.
