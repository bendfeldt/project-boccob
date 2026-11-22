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
| `incremental` | `clean.borgerportal.{citizens,cases,payments}`, `curated.borgerservice.fact_payment` | additive: automatic; anything else: migration |
| keystore (macros) | `curated.keystore.*` | migration |
| governance (python) | `raw.governance.erasure_requests` | evolves itself (pyiceberg `union_by_name`, nullable columns) |
| `table`, `dimension`, `view` | `clean.borgerportal.case_types`, `consolidated.party.person` (view), `curated.borgerservice.dim_*`, `dp_*` | rebuilt from their sources on every run - nothing to do (keys come from the keystore) |

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
   * never change a statement or directive of a released migration: the runner compares checksums
     and stops. The checksum covers the normalised SQL (comments and whitespace removed,
     placeholders unresolved) plus the directives, so fixing a comment is fine.

   Applied migrations are recorded in `curated.audit.schema_migrations` (version, name, checksum,
   statement count, time, principal, duration). The schema `curated.audit` is protected: only the
   pipeline service group `lakehouse-service` writes it (OPA). If the lake knows a migration that the
   deployed release does not (a rollback to an older image), the run stops: roll forward, or restore
   the lake together with the old image.

   After the migrations the runner compares the live columns of every incremental model with the
   model (drift check): a column that exists in the lake but not in the model would otherwise be
   kept, with its data, by `append_new_columns`, and a live type that differs from a `data_type`
   declared in the model yml - the run fails until a migration fixes it. A changed type of an
   undeclared column is refused when dbt runs the model (`trino__process_schema_changes` in
   `macros/utils/guards.sql`: `append_new_columns` would otherwise ignore the new type).
3. **No full refresh outside dev.** With `LAKEHOUSE_ENV` other than `dev` (unset counts as
   production) `dbt ... --full-refresh` of an incremental model is refused before anything runs
   (`on-run-start` hook) and again inside the materialization (`should_full_refresh` override, also
   for a `full_refresh: true` config). The integration harness and local development set
   `LAKEHOUSE_ENV=dev`.

## Where the migrations run

The Dagster asset `curated/audit/schema_migrations` (pool `lakehouse_writer`) applies the pending
migrations. Every dbt model without an upstream model depends on it, so in `transform` the migrations always run before the first model of a release. The job `migrate` runs
them alone. The `erasure` and `retention` jobs run them as their first step as well, because their
SQL is derived from the release's model metadata and may already refer to a new column.
All of them run in the `lakehouse_writer` pool (limit 1), so migrations never run concurrently
with each other or with another writer.

By hand: `make migrate-status` (read-only), `lakehouse-migrate --check` (exit 3 when pending) and,
in dev only, `make migrate` (`lakehouse-migrate`). Outside dev the CLI refuses to apply: it is not
serialised with the Dagster writers, and `@purge-history` uses a zero retention window - use the
Dagster job `migrate`. `make dbt-build` runs the migrations first in dev and refuses to build
elsewhere while migrations are pending.

## Release procedure

1. **Develop.** Change the models; for every non-additive change of a table in the first table above
   add the next `V<nnnn>__*.sql`. Run `make lint test` and `make integration`: the integration test
   builds the lake with the previous model shape, upgrades it with the new release (migrations through
   the `transform` job) and checks that keystore keys, SCD2 versions and the clean history survive. Add
   the previous shape of your change to `previous_release_project()` in `tests/integration/test_e2e.py`.
2. **Build and publish** the artifacts (`make publish`, see *Publishing* below); the platform
   signs the two digest-pinned references and takes them as variables.
3. **Before the deployment:** make sure a recent backup of the lake exists (platform: Polaris database,
   bucket versioning / off-site copy). A migration that rewrites data is not reversible otherwise.
4. **Deploy** the new image and bundle (IaC). Let running jobs finish; schedules may stay on - every
   writing job waits for the `lakehouse_writer` pool.
5. **Migrate.** Launch the Dagster job `migrate` (or let the next `transform` do it) and check the
   materialization metadata (`applied`, `history_purged`) and `curated.audit.schema_migrations`.
   A failure leaves the lake on the old shape (or, for a multi-statement migration, partially
   migrated - idempotent statements make the re-run safe): fix forward with a new release.
6. **Verify.** The next `transform` run must pass all asset checks (keystore integrity, stability
   since baseline, SCD2, usage).

## Publishing

```bash
BOCCOB_PLATFORM_ENV=<file> make publish     # build, smoke test, push image + OPA bundle, print both refs
```

`scripts/publish.sh` reads the environment's registry, its CA and the push credential from the
platform environment file (platform-contract.md, section 4) and calls `build-image.sh`,
`push-image.sh` and `push-opa-bundle.sh` (also usable alone: `make image image-push bundle
bundle-push` with the variables below). It prints the two digest-pinned references:

```
dagster_code_location_image = "<registry>/boccob/dagster-code-location@sha256:..."
opa_bundle_ref              = "<registry>/boccob/opa-policy-bundle@sha256:..."
```

**Signing is a platform function** (the platform holds the key): run
`make sign REFS="<image ref> <bundle ref>"` in the platform repository, then set the two
references as the environment's image and bundle variables and deploy. This repository never signs.

| Variable | Used by | Meaning |
|---|---|---|
| `REGISTRY` (required for push) | build, push | registry host[:port] (`make publish`: `BOCCOB_REGISTRY_HOST`) |
| `REGISTRY_NAMESPACE` | build, push | repository prefix, default `boccob` |
| `VERSION` | build, push | artifact version, default `0.1.0-<git describe>` |
| `REGISTRY_CA_FILE` | push | PEM of the CA that signed the registry certificate. Passed per call: podman gets `--cert-dir` (a temporary directory with `ca.crt`), oras `--ca-file`. Nothing is written to `/etc` |
| `REGISTRY_USERNAME` / `REGISTRY_PASSWORD` | push | login into a temporary auth file (podman `REGISTRY_AUTH_FILE`, oras `--registry-config`) that is removed when the script ends: no push credential stays on the build host. Without them an existing login is used |
| `REGISTRY_HOST` | push | host[:port] appended to `NO_PROXY`/`no_proxy` for the call (default: host part of `REGISTRY`) |
| `REGISTRY_TLS_VERIFY=false`, `REGISTRY_PLAIN_HTTP=1` | push | local test registries only |
| `BUILD_CA_BUNDLE`, `BUILD_PROXY`, `BUILD_NETWORK`, `UV_DEFAULT_INDEX`, `PYTHON_IMAGE`, `UV_IMAGE` | build | trust, proxy, package index and base images *inside* the image build (see `scripts/build-image.sh`); `make publish` derives proxy and CA from `HTTPS_PROXY` / `SSL_CERT_FILE` |
| `SCAN_STRICT` | build | `1` (default): fixable HIGH/CRITICAL findings fail the build when trivy is installed; `0` reports only |
| `BUILD_STORAGE_ROOT`, `KEEP_LOCAL_IMAGE`, `SKIP_BUILD` | publish | separate podman storage for a disk-constrained host; keep the local image; push what was built |

`push-opa-bundle.sh` always rebuilds the bundle first, so a release never ships an older policy
under a new version. SBOMs (`dist/image/sbom.*.json`) and the trivy report (`dist/image/trivy.json`)
are produced when `make tools-supply-chain` installed syft and trivy.

**Architectures.** The image is built natively for the machine that runs podman: x64 (linux/amd64)
on an x64 Linux host, arm64 on an arm64 Linux host or Apple Silicon (inside the podman machine VM).
A cluster of the other architecture gets its image from a native build on such a machine; there is
no cross-build or emulation. `make lint` checks (`scripts/check-wheels.py`) that every locked Python
dependency has cp313 manylinux x86_64 and aarch64 wheels, so the arm64 build needs no compiler.
`dbt-core-experimental-parser` is the one sdist: its build backend downloads a prebuilt,
sha256-checked binary for the platform from the dbt GitHub release; air-gapped builds need that
file mirrored. `make tools` / `tools-supply-chain` install the opa, oras, syft and trivy builds for
this machine (linux/amd64, linux/arm64, darwin/arm64).

## Break-glass repair of protected schemas

`curated.keystore`, `curated.audit` and `raw.governance` accept writes only from the service group
(`lakehouse-service`, i.e. `svc-dagster`); platform administrators have no bypass. A repair
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
