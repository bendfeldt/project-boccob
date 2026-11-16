# Platform contract

This is the complete interface between the solution repository and the platform (IaC) repository.
The platform consumes two versioned artifacts and provides a set of environment variables and
service configurations. Nothing else is shared; there are no cross-repository file references.

## 1. Artifacts published by this repository

| Artifact | Reference | Built by | Pushed by |
|---|---|---|---|
| Dagster code location image | `<REGISTRY>/<REGISTRY_NAMESPACE>/dagster-code-location:<VERSION>` | `make image` | `make image-push` |
| OPA policy bundle (OCI artifact) | `<REGISTRY>/<REGISTRY_NAMESPACE>/opa-policy-bundle:<VERSION>` | `make bundle` | `make bundle-push` |

* `REGISTRY` is the Zot host (required for pushing), `REGISTRY_NAMESPACE` defaults to `boccob`,
  `VERSION` defaults to `0.1.0-<git describe>`.
* After a push the digest-pinned references are written to `dist/image/image.ref` and
  `dist/opa/bundle.ref`. These two strings are what the IaC repository takes as variables.
* `make image` also writes `dist/image/sbom.spdx.json` and `sbom.cdx.json` (syft) and
  `dist/image/trivy.json` (trivy) when the tools are installed (`make tools-supply-chain`).
  With `COSIGN_KEY` set, `make image-push` signs the image and attaches the SBOM attestation.

### 1.1 Code location image

| Property | Value |
|---|---|
| Contents | python 3.13, locked dependencies (`uv.lock`), packages `ingestion`, `governance`, `orchestration`, `lakehouse`, dbt project in `/app/transform` with a pre-built manifest |
| Default command | `dagster api grpc --host 0.0.0.0 --port 4000 --module-name orchestration.definitions` |
| Port | 4000 (gRPC) |
| Code location module | `orchestration.definitions` (attribute `defs`) |
| User | uid/gid `10001`, no shell, no capabilities needed |
| Root filesystem | read-only capable; **mount a writable `emptyDir` on `/tmp`** (dbt target/logs, dlt working dir, `HOME`) |
| CLI entry points | `lakehouse-ingest`, `lakehouse-erase`, `lakehouse-retention`, `lakehouse-maintain`, `lakehouse-pii`, `dbt`, `dagster` |
| Base images | pinned by digest in `images/dagster-code-location/Containerfile`; overridable build args `PYTHON_IMAGE`, `UV_IMAGE`, `UV_DEFAULT_INDEX` for mirrored/air-gapped builds |

Run pods launched by Dagster (K8sRunLauncher) must use the same image and the same environment
(secrets included) as the code location deployment.

Dagster jobs, schedules (all `STOPPED` unless `SCHEDULES_ENABLED=true`):

| Job | Schedule (Europe/Copenhagen) | Purpose |
|---|---|---|
| `daily_refresh` (daily partitions) | 02:15 | land + index all sources, dbt build with tests |
| `ingest_<source>` (daily partitions) | - | only the two ingestion stages of one source |
| `transform` | - | dbt build only |
| `erasure` | 00:30 | process queued right-to-erasure requests |
| `retention` | 01:00 | apply the retention policy |
| `iceberg_maintenance` | Sunday 04:00 | optimize, expire snapshots, remove orphan files |

All assets and ops that write use the Dagster concurrency pool **`lakehouse_writer`**. The platform
must limit it to 1 (`dagster instance concurrency set lakehouse_writer 1`, or the equivalent
instance setting) so that the erasure job, maintenance and the keystore have a single writer.

### 1.2 OPA policy bundle

* Format: a standard OPA bundle `bundle.tar.gz` with roots `trino` and `lakehouse`, pushed with
  `oras` as OCI artifact (config media type `application/vnd.oci.image.config.v1+json`, one layer
  `application/vnd.oci.image.layer.v1.tar+gzip`) - the layout OPA's OCI bundle downloader expects.
  The same tarball is available as a file in `dist/opa/bundle.tar.gz` (+ `.sha256`).
* Trino `access-control.properties` (OPA service assumed at `http://opa.query.svc:8181`):

```properties
access-control.name=opa
opa.policy.uri=http://opa.query.svc:8181/v1/data/trino/allow
opa.policy.batched-uri=http://opa.query.svc:8181/v1/data/trino/batch
opa.policy.row-filters-uri=http://opa.query.svc:8181/v1/data/trino/rowFilters
opa.policy.batch-column-masking-uri=http://opa.query.svc:8181/v1/data/trino/batchColumnMasks
```

  (`opa.policy.column-masking-uri=.../v1/data/trino/columnMask` is the non-batch alternative; Trino
  accepts only one of the two - the harness sets both and Trino uses the batch one.)
  `opa.allow-permission-management-operations` must stay `false`.
* The policy decides on `input.context.identity.groups`. Trino must therefore resolve the Keycloak
  groups of a user (group provider), with exactly these names:
  `platform-admin`, `data-engineer`, `analyst`, `pii-reader`, `auditor`.
  Where Trino cannot resolve groups for a principal (service accounts), the platform can overlay
  `data.lakehouse.config.principals` = `{"<user>": {"groups": [...]}}` in OPA.
* The service account used by Dagster/dbt (`TRINO_USER`) must be in **`data-engineer` and
  `pii-reader`** - otherwise it would transform masked values.

## 2. Environment variables expected by the image

All configuration is read from the environment (Kubernetes Secret/ConfigMap fed from OpenBao through
the External Secrets Operator). Variables read by the python code also accept `<NAME>_FILE` pointing
to a mounted file; the dbt profile reads plain variables only.

| Variable | Secret | Default | Meaning |
|---|---|---|---|
| `S3_ENDPOINT_URL` | | (AWS) | RustFS S3 endpoint, e.g. `https://rustfs.storage.svc:9000` |
| `S3_ACCESS_KEY_ID` | yes | | access key with read/write on the raw bucket (dlt, erasure, retention) and read on all layer buckets if `ERASURE_VERIFY_STORAGE=true` |
| `S3_SECRET_ACCESS_KEY` | yes | | |
| `S3_REGION` | | `us-east-1` | region name configured in RustFS / Polaris |
| `RAW_BUCKET` | | `lakehouse-raw` | landing bucket |
| `RAW_INDEX_PREFIX` | | `iceberg` | prefix of the raw Iceberg tables; must equal the path part of the raw catalog's base location |
| `POLARIS_URI` | | | Iceberg REST endpoint, e.g. `https://polaris.catalog.svc:8181/api/catalog` |
| `POLARIS_CLIENT_ID` | yes | | Polaris principal for ingestion (index stage) |
| `POLARIS_CLIENT_SECRET` | yes | | |
| `POLARIS_SCOPE` | | `PRINCIPAL_ROLE:ALL` | |
| `POLARIS_OAUTH2_SERVER_URI` | | derived from `POLARIS_URI` | token endpoint when it is not Polaris' own |
| `RAW_CATALOG` `CLEAN_CATALOG` `CONSOLIDATED_CATALOG` `CURATED_CATALOG` | | `raw` `clean` `consolidated` `curated` | Polaris/Trino catalog per layer |
| `TRINO_HOST` | | | Trino coordinator host |
| `TRINO_PORT` | | `8443` (https) / `8080` (http) | |
| `TRINO_USER` | | | service account, e.g. `svc-dagster` |
| `TRINO_HTTP_SCHEME` | | `https` (python) | `http` only without authentication |
| `DBT_TARGET` | | `default` | dbt authentication profile: `default` (none), `password`, `jwt` |
| `TRINO_PASSWORD` | yes | | for `DBT_TARGET=password` and the python client |
| `TRINO_JWT_TOKEN` | yes | | for `DBT_TARGET=jwt` and the python client |
| `TRINO_CA_BUNDLE` / `LAKEHOUSE_CA_BUNDLE` | | system trust | path of the internal CA bundle (Trino client, s3fs, pyiceberg REST) |
| `PSEUDONYM_PEPPER` | **yes** | | >= 32 characters; key of the HMAC pseudonymisation. Losing it breaks key continuity, leaking it weakens pseudonymisation - store in OpenBao, back it up, never rotate without a re-index plan (docs/gdpr.md) |
| `DBT_THREADS` | | `4` | dbt concurrency |
| `SCHEDULES_ENABLED` | | `false` | start schedules automatically |
| `SCHEDULE_TIMEZONE` | | `Europe/Copenhagen` | |
| `INGEST_START_DATE` | | `2026-01-01` | first daily partition |
| `ERASURE_VERIFY_STORAGE` | | `false` | after an erasure, scan all layer buckets for remains (slow, thorough) |
| `INGEST_<SOURCE>_<OPTION>` | | source.yml | per-source option overrides |

For TLS with the internal CA also set the generic variables honoured by the underlying libraries:
`SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE` and `AWS_CA_BUNDLE` (botocore, pyarrow, requests).

Set by the image, normally left alone: `DBT_PROJECT_DIR=/app/transform`, `DBT_PROFILES_DIR=/app/transform`,
`DBT_TARGET_PATH=/tmp/dbt-target`, `DBT_LOG_PATH=/tmp/dbt-logs`, `DLT_DATA_DIR=/tmp/dlt`,
`DLT_PIPELINES_DIR=/tmp/dlt/pipelines`, `HOME=/tmp`, `DAGSTER_HOME=/tmp` (override with the instance config).

## 3. What the platform must provide

Verified against the integration harness (RustFS v1.0.0-rc.5, Polaris 1.9.0, Trino 484, OPA 1.22.4);
the harness files in `tests/integration/` are a working reference configuration.

**Object storage (RustFS)**
* Buckets `lakehouse-raw`, `lakehouse-clean`, `lakehouse-consolidated`, `lakehouse-curated`.
* No object lock in compliance mode on `lakehouse-raw` beyond the raw retention window: the erasure
  and retention jobs must be able to delete native files (they delete all object versions).

**Polaris**
* One internal catalog per layer, named as the layer, with
  `default-base-location = s3://lakehouse-<layer>/iceberg` and `allowedLocations = [s3://lakehouse-<layer>/]`.
  The raw native files live in the same bucket under `s3://lakehouse-raw/<source>/...`, outside the
  catalog base location.
* Catalog property `polaris.config.drop-with-purge.enabled=true` (or the realm feature
  `DROP_WITH_PURGE_ENABLED`). Without it Trino `DROP TABLE` fails with 403 and dbt cannot remove its
  temporary tables.
* Static object store credentials (storage config `stsUnavailable: true`, `pathStyleAccess: true`,
  `endpoint` / `endpointInternal` of RustFS). Credential vending is not used by this solution.
* Principals: one for Trino, one for ingestion (`POLARIS_CLIENT_ID`), each with a catalog role that
  grants `CATALOG_MANAGE_CONTENT` on the catalogs they use (ingestion: `raw` only).

**Trino**
* Catalogs `raw`, `clean`, `consolidated`, `curated`: Iceberg connector, REST catalog (see
  `tests/integration/trino/catalog.properties.tpl`), `iceberg.rest-catalog.vended-credentials-enabled=false`,
  native S3 file system with path-style access.
* OPA access control as in section 1.2; a group provider for the Keycloak groups.
* An authentication method for the service account (`password` or `jwt` dbt target). A static JWT
  expires: prefer password/certificate authentication for `svc-dagster`, or provide token refresh.
* The jobs lower `expire_snapshots_min_retention` / `remove_orphan_files_min_retention` per session
  (catalog session properties); no catalog level change is needed. The policy only lets principals
  with write access set them.

**Dagster**
* User code deployment from the image; run launcher pods with the same image and environment.
* Pool `lakehouse_writer` limited to 1.
* Egress (NetworkPolicy) from the orchestration namespace to Trino, Polaris, RustFS and the Dagster database.

**Secrets (OpenBao KV -> ESO -> Secret -> env)**
`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `POLARIS_CLIENT_ID`, `POLARIS_CLIENT_SECRET`,
`TRINO_PASSWORD` or `TRINO_JWT_TOKEN`, `PSEUDONYM_PEPPER`.

**Audit**
* The pepper never appears in SQL text (pseudonyms are computed in the index stage), so Trino query
  logs can be shipped to the central audit store as they are.
* Erasure executions are recorded in `curated.audit.erasure_log` (no identifiers) and logged as JSON
  on stdout of the run pod.
