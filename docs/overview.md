# Overview: what this repository does

This page is the map of the solution repository for someone new to it. It follows one example,
the demo source "Borgerportal", from the source system to the dashboard table. Each section links to
the document with the details.

The **platform** (Kubernetes, storage, catalog, Trino, Dagster, OpenBao, Keycloak) is built by a
separate repository, `project-boccob-iac`. Clone it next to this one. Its `docs/overview.md`
explains the platform, and its `docs/getting-started.md` sets everything up on your machine. This
repository holds only the **data code**. The two repositories share no files; they meet only
through the published artefacts and one environment file ([platform-contract.md](platform-contract.md)).

## Words you will meet

| Word | Meaning here |
|---|---|
| **Lakehouse** | Files in object storage (S3) that behave like database tables. Here: four buckets on RustFS, one per layer |
| **Iceberg** | The open table format of those files. An Iceberg table is data files plus metadata files that say which data files make up the table now (a *snapshot*). Old snapshots allow "time travel" until they are expired |
| **Catalog** | The service that knows where each Iceberg table's current metadata is. Here Apache Polaris, spoken to through the Iceberg **REST catalog** API. One catalog per layer: `raw`, `clean`, `consolidated`, `curated` |
| **Trino** | The SQL engine. Every table is `<catalog>.<schema>.<table>`, e.g. `clean.borgerportal.citizens` |
| **dlt** | Python library for loading data. Used for the two raw stages: LAND (store the delivered files) and INDEX (make them queryable) |
| **dbt** | SQL transformation tool. Every table from `clean` on is a dbt *model* (a `SELECT` in `transform/models/`) |
| **Dagster** | The scheduler. It shows every table as an *asset*, runs *jobs* (sets of assets or steps) on *schedules*, and keeps the run history |
| **OPA** | Open Policy Agent. Trino asks it, for every query, what the user may see. The policies are in `governance/policies/` and ship as a *bundle* |
| **Pseudonymisation** | Replacing a person identifier (CPR number) by a keyed hash. The key is the **pepper**, a secret held by the platform (OpenBao) |
| **Masking** | OPA replaces the value of a personal-data column with `*****`, a hash or `NULL` for users without the `pii-reader` group |
| **Surrogate key** | A meaningless integer that identifies a row of a dimension (e.g. `citizen_key = 17`). Facts point to it |
| **Keystore** | The tables that remember every surrogate key ever handed out, so keys never change when a dimension is rebuilt ([keystore.md](keystore.md)) |
| **SCD2 / SCD1** | Slowly changing dimension type 2: a new row (new key, with `valid_from`/`valid_to`) for every change, so history is kept. Type 1: one row per member, overwritten |
| **Migration** | A numbered SQL file that changes the shape of a table that keeps history (`transform/migrations/V0001__*.sql`) ([releases.md](releases.md)) |
| **Code location** | The container image of this repository that Dagster loads (`dagster-code-location`) |

## The data flow, with the demo source

The demo source is a fictional municipal system, **Borgerportal**. Its generator
(`ingestion/sources/borgerportal/generator.py`) is deterministic and 100 % synthetic. Each daily
delivery has four entities, in four formats on purpose:

| Entity | Format | Person identifier |
|---|---|---|
| `citizens` | JSON API envelope (changed records and tombstones) | `cpr` |
| `cases` | XML export | `cpr` |
| `payments` | CSV with `;`, Danish decimals and dates | `cpr` |
| `case_types` | Parquet, full snapshot | none |

The source is described in one file, `ingestion/sources/borgerportal/source.yml`.

```
 Borgerportal delivery (json, xml, csv, parquet)
     |
     | LAND  (dlt, ingestion/framework/landing.py)        Dagster job ingest_borgerportal, 02:15
     v
 RAW   s3://lakehouse-raw/borgerportal/<entity>/load_date=YYYY-MM-DD/<load_id>/<file as delivered>
     |
     | INDEX (dlt, ingestion/framework/indexing.py)       same job; CPR -> pseudonym here (pepper)
     v
 RAW   raw.borgerportal.{citizens,cases,payments,case_types}     one row per record, payload as JSON text
     |
     | dbt (transform/models/clean/borgerportal/)         Dagster job transform, 04:00
     v
 CLEAN         clean.borgerportal.{citizens,cases,payments,case_types}   typed, deduplicated, no CPR
     |
     | dbt (transform/models/consolidated/party/)
     v
 CONSOLIDATED  consolidated.party.person                                  the person as a business entity
     |
     | dbt (transform/models/curated/borgerservice/)
     v
 CURATED       curated.borgerservice.{dim_citizen, dim_case_type, dim_date, fact_payment,
                                      dp_payment_summary_v1}
               curated.keystore.{dim_citizen, dim_case_type}     surrogate keys
               curated.audit.{erasure_log, schema_migrations}    audit tables
```

What happens in each layer (details: [data-layers.md](data-layers.md)):

1. **raw, LAND.** The files are stored byte for byte, never overwritten. A manifest with the
   checksum of each file is written last; a load without a manifest does not count.
2. **raw, INDEX.** Every record becomes one row with the record as JSON text (`payload`), where it
   came from (`_raw_file`, `_raw_row`, `_load_id`) and `_subject_keys`: the pseudonyms of the person
   identifiers named in `source.yml`. **This is where the CPR number is pseudonymised**, in Python,
   so the pepper never appears in SQL.
3. **clean.** One dbt model per entity: parse, type, deduplicate, denormalise. Persons are
   identified only by the pseudonym (`citizen_id`). The CPR number is not copied into clean.
   `citizens` keeps every delivered state, which is the history SCD2 needs.
4. **consolidated.** Business entities independent of the source system. `consolidated.party.person`
   is the person; a second source would add a `UNION ALL` branch. These are persisted Iceberg tables
   (never views; `make lint` checks it), because they hold personal data with their own erasure and
   retention rules. The layer is optional: `dim_case_type` and `fact_payment` read clean directly.
5. **curated.** The dimensional model. `dim_citizen` (SCD2) and `dim_case_type` (SCD1) take their
   surrogate keys from the keystore; `fact_payment` points to the version of the citizen that was
   valid on the payment date; `dp_payment_summary` (version 1, table `dp_payment_summary_v1`) is the
   published data product, with a small-cell flag.

In Dagster the same chain is the asset graph: `landing/borgerportal/<entity>` ->
`raw/borgerportal/<entity>` -> `clean/...` -> `consolidated/...` -> `curated/...`. dbt tests show up as
asset checks.

## Jobs and schedules

Defined in `orchestration/jobs.py`. Times are Europe/Copenhagen. Schedules ship **stopped**; the
platform starts them by setting `SCHEDULES_ENABLED=true` (test and prod do, dev does not: on dev you
launch jobs by hand in the Dagster UI or with `make e2e`).

| Time | Job | What it does |
|---|---|---|
| 00:30 | `erasure` | apply pending migrations, then process queued erasure requests |
| 01:00 | `retention` | apply pending migrations, then delete expired rows and files, queue deregistered persons for erasure |
| 02:15 | `ingest_borgerportal` | LAND + INDEX of yesterday's delivery (one job and schedule per source, so a failing source holds back no other) |
| 04:00 | `transform` | apply pending migrations, then `dbt build` of clean, consolidated, curated with all tests |
| Sunday 05:00 | `iceberg_maintenance` | compact files, expire snapshots, remove orphan files and old object versions |
| (by hand) | `migrate` | apply pending migrations only; run once after deploying a release |

Every job that writes Iceberg tables uses the Dagster pool `lakehouse_writer`, which the platform
limits to 1. So two writers never run at the same time (the keystore relies on it).

## Personal data: where it is protected

Details: [gdpr.md](gdpr.md).

| Where | What happens |
|---|---|
| `source.yml` (`identifiers: {cpr: cpr}`) | declares which field is a person identifier |
| raw INDEX | `HMAC-SHA256(pepper, CPR)` -> `_subject_keys`; the record itself is stored untouched |
| raw `payload` | masked by OPA for everyone without `pii-reader` (the text cannot be masked field by field) |
| clean and later | only the pseudonym (`citizen_id`, `person_id`); no CPR column |
| every column of clean, consolidated, curated | classified in the dbt model yml (`meta.pii`: `none`, `indirect`, `direct`, `sensitive`). `direct` and `sensitive` are masked by OPA for users without `pii-reader`. An unclassified column fails `make lint` and is masked at query time |
| `curated.borgerservice.dp_payment_summary_v1` | cells with fewer than 5 persons are hidden (OPA row filter) from readers without `platform-admin`, `data-engineer` or `pii-reader` |
| Trino query text | never contains a pseudonym, CPR number or the pepper from this code; the platform also redacts such literals in its audit log |

Who sees what: `analyst` reads curated only (masked), `pii-reader` adds clear values, `auditor` reads
everything (masked) plus the query history, `data-engineer` and `platform-admin` read and write all
layers but cannot write the protected schemas. Only the pipeline account `svc-dagster` (group
`lakehouse-service`) writes `curated.keystore`, `curated.audit` and `raw.governance`.

**Erasure (GDPR art. 17).** `lakehouse-erase` puts the pseudonym of a person into the queue
`raw.governance.erasure_requests`. The nightly `erasure` job deletes the person's rows in raw, clean
and consolidated, anonymises the facts, turns the dimension members into "erased" members (the
keystore keeps the integer key but forgets the person), deletes the raw files, purges the old
snapshots and object versions of the touched tables and writes an audit row to
`curated.audit.erasure_log`. `make erase-dry-run` shows the plan without an identifier.

**Retention.** `governance/retention.yml` (per layer) plus `meta.gdpr` on each model (per table).
Demo values: raw files 30 days, raw index rows 90 days, payments and cases 5 years, persons until one
year after deregistration (then a *deregistration* erasure: identifiers go, bookkeeping rows stay
anonymised until their own 5 years are over). `make retention-dry-run` reports what would be deleted.

## Adding a source

One directory, one YAML file, one Python function, and dbt models for the layers you want. Dagster
picks the source up by itself: it gets `landing/<source>/...` and `raw/<source>/...` assets and its
own `ingest_<source>` job and 02:15 schedule. Step by step: [adding-a-source.md](adding-a-source.md).

## Releasing a change

A release is two artefacts: the code location image and the OPA bundle. Most releases change only
code. When a release changes the **shape** of a table that keeps history (incremental models,
keystore), the lake must be migrated in place, because it cannot be rebuilt from raw (raw keeps
files only 30 days).

- Adding a column: automatic (`on_schema_change='append_new_columns'`).
- Anything else (drop, rename, retype, backfill): a new file `transform/migrations/V<nnnn>__<slug>.sql`.
  Released so far: `V0001` (drop the CPR column from `clean.borgerportal.citizens`, with
  `@purge-history`), `V0002` (add `_anonymised_at` to clean cases and payments), `V0003` (drop two
  retired governance tables). Applied migrations are recorded in `curated.audit.schema_migrations`
  and must never be edited.
- A full refresh of incremental models is refused outside dev.

The procedure (backup, publish, sign, deploy, `migrate`, verify) is in [releases.md](releases.md).

## The local developer loop

Two ways to run the code: a throwaway harness on your machine, or the real dev platform.

**1. On the harness (no platform needed).** Containers for RustFS, Polaris, OPA and Trino in podman.

```bash
make setup         # virtualenv from uv.lock (needs uv, python 3.13)
make tools         # pinned opa + oras into .tools/bin
make lint test     # ruff, shellcheck, OPA, dbt parse, classification; pytest, opa test, dagster validate
make integration   # whole flow on a throwaway harness, then removes it (~5 min, ~5 GB RAM)
make harness-up    # or keep a harness running and use make ingest / dbt-build / dagster-dev against it
make harness-down
```

**2. On the dev platform** (after the platform's getting-started guide, which ends with
`make dev-env ENV=dev` in the platform repository):

```bash
export BOCCOB_PLATFORM_ENV=~/.boccob/dev/developer.env   # written by the platform; never inside a checkout
make publish       # build + push image and OPA bundle to the dev registry, prints two digest refs
# platform repository: make sign, put the refs into the dev tfvars, deploy stacks 21 and 22
#                      (its getting-started guide shows the exact commands)
# Dagster UI: launch the job `migrate` when the release has new migrations
make e2e           # loads, keystore, masking per user, erasure, purge credential, full-refresh guard
CONFIRM=dev make reset-dev-data   # start the dev lake from scratch (e.g. after a migration checksum change)
```

`make help` lists every target.
