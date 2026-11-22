# Boccob lakehouse - solution

Data code of the Boccob lakehouse: ingestion (dlt), transformation (dbt on Trino/Iceberg), governance
(OPA policies, retention, right to erasure) and orchestration (Dagster). The platform itself (k3s,
RustFS, Polaris, Trino, OPA, Dagster, OpenBao, Keycloak) lives in the IaC repository and consumes
this repository only through two published artifacts, see [docs/platform-contract.md](docs/platform-contract.md).

```
ingestion/       dlt framework (LAND native files, INDEX schema-on-read) + sources/<name>/
transform/       dbt project: clean, consolidated, curated, keystore macros, tests, migrations/
governance/      OPA policies for Trino, PII classification, erasure, retention, Iceberg maintenance
orchestration/   Dagster code location: assets, checks, jobs, schedules
images/          Containerfile of the code location image
lakehouse/       shared settings (environment only), Trino client, object store helpers
scripts/         every operation as a bash script; the Makefile wraps them
tests/           unit tests and the podman integration harness
docs/            data-layers, keystore, gdpr, adding-a-source, releases, platform-contract
```

## Quick start

To run the whole platform (cluster, Trino, Dagster, ...) with this code on it, follow the
[getting-started guide in the IaC repository](https://github.com/bendfeldt/project-boccob-iac/blob/main/docs/getting-started.md).
The commands below only cover this repository on its own.

Requirements: `uv`, `podman`, GNU make, python 3.13.

```bash
make setup          # virtualenv from uv.lock
make tools          # pinned opa + oras into .tools/bin (checksums verified; linux amd64/arm64, macOS arm64)
make lint           # ruff, shellcheck, opa fmt/check, dbt parse, PII classification
make test           # pytest, opa test, dbt parse, dagster definitions validate
make integration    # end-to-end on a throwaway podman harness (RustFS, Polaris, OPA, Trino)
make help           # all targets
```

`make integration` starts containers named `sol-*` on the network `sol-test`, runs the whole flow
and removes everything again (about 5 minutes, roughly 5 GB RAM). `KEEP_HARNESS=1` keeps it running;
`make harness-up` / `make harness-down` control it by hand, and with the harness up the other
targets (`make ingest`, `make dbt-build`, `make dagster-dev`, ...) work against it because
`scripts/lib.sh` loads `~/.local/state/boccob/solution/env` (your own settings) or the harness env next to it. No secret file may live in the checkout: `.env`, `.harness/` or `.dlt/secrets.toml` in the repo make the scripts stop.

## The demo flow

A fictional municipal system, "Borgerportal", delivers citizens (JSON API envelope), cases (XML),
payments (CSV with Danish number and date formats) and case types (parquet). The generator is
deterministic and every delivery changes the world: citizens move, are deleted, payments arrive
late, and some payments arrive before the citizen's master data. All data is synthetic.

| Layer | Objects |
|---|---|
| raw | `s3://lakehouse-raw/borgerportal/<entity>/load_date=.../<load_id>/...` and `raw.borgerportal.{citizens,cases,payments,case_types}` |
| clean | `clean.borgerportal.{citizens,cases,payments,case_types}` |
| consolidated | `consolidated.party.person` (view) |
| curated | `curated.borgerservice.{dim_citizen,dim_case_type,dim_date,fact_payment,dp_payment_summary_v1}`, `curated.keystore.*`, `curated.audit.*` |

Read more:

* [docs/data-layers.md](docs/data-layers.md) - what each layer contains and how data moves
* [docs/keystore.md](docs/keystore.md) - stable integer surrogate keys, SCD2/SCD1, inferred members, limits
* [docs/gdpr.md](docs/gdpr.md) - classification, masking, pseudonymisation, CPR, retention, erasure
* [docs/adding-a-source.md](docs/adding-a-source.md) - a new source in one YAML file and one function
* [docs/releases.md](docs/releases.md) - schema evolution without data loss: migrations, release procedure, break-glass
* [docs/platform-contract.md](docs/platform-contract.md) - artifacts, environment variables, platform requirements

## Building and publishing the artifacts

```bash
make tools-supply-chain                               # syft, trivy, cosign (optional)
REGISTRY=zot.dev.lakehouse.internal make image        # build, smoke test, SBOM, scan
REGISTRY=zot.dev.lakehouse.internal make image-push   # -> dist/image/image.ref (digest)
make bundle                                           # dist/opa/bundle.tar.gz
REGISTRY=zot.dev.lakehouse.internal make bundle-push  # -> dist/opa/bundle.ref (digest)
```

The build needs a python package index. Options for restricted networks are documented at the top
of `scripts/build-image.sh` (`UV_DEFAULT_INDEX` for an internal mirror, `BUILD_CA_BUNDLE`,
`BUILD_PROXY`, `BUILD_NETWORK`).

## Configuration

Everything is configured through environment variables; there are no credentials in this
repository. The full list is in [docs/platform-contract.md](docs/platform-contract.md). The
integration harness generates throwaway credentials into `~/.local/state/boccob/solution/harness/env`, outside the repo.

## Conventions

* Dependencies are pinned in `pyproject.toml` and locked in `uv.lock`; tools are pinned in
  `scripts/install-tools.sh`; base images are pinned by digest in the Containerfile.
* A column without a PII classification fails `make lint`.
* After changing dbt meta (classification, row filters) run `make classification` and commit the
  regenerated `governance/policies/lakehouse/classification/data.json`.
