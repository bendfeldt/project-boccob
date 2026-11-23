# Personal data: classification, masking, pseudonymisation, retention, erasure

Scope: GDPR and the Danish Data Protection Act (databeskyttelsesloven, incl. section 11 on CPR
numbers). This document describes the technical controls implemented in this repository. It is not
legal advice; the controller decides purposes, legal bases and retention periods and records them
in the record of processing activities.

| Control | Where | Enforced by |
|---|---|---|
| Classification of every column | dbt `config.meta` in the schema files | `make check-classification` (lint), also against live tables with `--live` |
| Masking by role | OPA policy `governance/policies/trino` + generated classification data | Trino OPA plugin (column masks) |
| Layer based access | OPA policy + `lakehouse/config/data.yaml` | Trino OPA plugin |
| Pseudonymised keys from clean onward | index stage (`ingestion/framework/pseudonym.py`) | dbt tests `pseudonym_format` |
| CPR confined to the raw layer | native files and the masked `payload` of the raw index | integration test (no CPR column or value in clean, consolidated or curated) |
| Retention | `governance/retention.yml` + dbt `meta.gdpr` | Dagster job `retention` |
| Right to erasure | `governance/erasure` | CLI `lakehouse-erase`, Dagster job `erasure`, audit table |
| Disclosure control for published aggregates | `dp_payment_summary.is_small_cell` + row filter | Trino OPA plugin (row filters), see below |

## Classification

Every column of every clean, consolidated and curated model declares:

```yaml
- name: cpr_number
  config:
    meta: {pii: direct, cpr: true, mask: redact}
```

| `pii` | Meaning | Masked for users without `pii-reader` |
|---|---|---|
| `none` | no personal data | no |
| `indirect` | identifies only in combination, or pseudonymised (postal code, case number, pseudonym key, amounts) | no |
| `direct` | identifies a person (CPR, name, address line, e-mail, phone, birth date) | yes |
| `sensitive` | special categories (GDPR art. 9) or criminal offence data (art. 10) | yes |

Additional keys: `cpr: true` (CPR number; must be `direct` or `sensitive`), `pseudonymised: true`,
`mask: null | redact | email | hash`. Model level: `meta.gdpr` (`subject_column`, `erasure`,
`retention_days`, `retention_column`, `anonymise_columns`) and `meta.row_filter`.

`lakehouse-pii check` fails when a column has no classification, an invalid level or mask, when a
model holds person-bound data without an erasure rule, or when the generated OPA data file is stale.
`lakehouse-pii generate` (`make classification`) writes
`governance/policies/lakehouse/classification/data.json`, which is committed and shipped in the OPA
bundle. A column added to a table without classification is therefore caught twice: by the lint
check, and at query time because unclassified columns in clean, consolidated and curated are masked
(`unclassified: mask`).

## Access and masking

Groups are the Keycloak groups of the architecture contract.

| Group | raw | clean | consolidated | curated | Notes |
|---|---|---|---|---|---|
| `platform-admin` | rw | rw | rw | rw | no automatic PII visibility; may kill queries, read system information; no writes to protected schemas |
| `data-engineer` | rw | rw | rw | rw | incl. table maintenance procedures; reads `curated.keystore`; no writes to protected schemas; cannot lower the retention floor |
| `analyst` | - | - | - | read | not `curated.keystore`, not `curated.audit` |
| `auditor` | read | read | read | read | read-only, sees all users' queries (`system.runtime`), reads the erasure log and queue |
| `lakehouse-service` | rw | rw | rw | rw | pipeline service accounts only (`svc-dagster`): the only writer of the protected schemas `curated.keystore`, `curated.audit`, `raw.governance`, and the only principal that may lower the snapshot/orphan retention floor |
| `pii-reader` | | | | | modifier only: removes column masks and row filters marked exempt; grants no access by itself |

* Everything not explicitly allowed is denied; users without one of these groups cannot run queries.
  Impersonation, catalog management and authorization changes are denied for everyone.
* Small cells: `dp_payment_summary` hides cells of fewer than 5 persons from every reader without
  `platform-admin`, `data-engineer` or `pii-reader`. That protects what is *published* from the
  product (dashboard, open-data export: they must read the product, never the fact). It does not
  stop an analyst from aggregating `fact_payment` and `dim_citizen` (pseudonym, postal code, birth
  year, amounts) down to single persons: analysts are internal staff authorised for pseudonymised
  row-level data, under the controller's access review. If that is not intended, move the data
  products to their own schema and grant `analyst` read on that schema only.
* Masks: `redact` -> `'*****'`, `email` -> local part replaced, `hash` -> SHA-256, `null` and every
  non-text column -> `NULL`. In `raw`, `payload` is masked for everyone without `pii-reader`
  (the record text cannot be masked field by field).
* Pipeline service accounts must be in `data-engineer` **and** `pii-reader`; a masked pipeline would
  write masked values downstream.
* 41 policy unit tests (`make opa-test`); the same rules are exercised through a real Trino in the
  integration test.

## Pseudonymisation and CPR handling

* A person identifier is replaced by `hex(HMAC-SHA256(pepper, normalised identifier))`. CPR numbers
  are normalised by removing the dash. Because the CPR number space is small, a plain hash would be
  reversible by enumeration; the keyed construction is not, as long as the pepper is secret.
* The pepper is a runtime secret (`PSEUDONYM_PEPPER`, from OpenBao). It is used only inside the
  index stage and the erasure tool. **It never appears in SQL**, so it cannot end up in Trino query
  logs, dbt artefacts or the audit store. This is why the HMAC is computed during indexing
  (`raw.<source>.<entity>._subject_keys`) instead of in a dbt macro: Trino has no way to pass a
  secret into a query without it becoming part of the query text.
* Which field of a record is a person identifier is declared per entity in the source spec
  (`identifiers: {cpr: cpr}`); the record itself is stored untouched.
* From clean onward, tables are joined on the pseudonymised key (`citizen_id`, `person_id`). The CPR
  number is not carried into clean (data minimisation): it exists only in the raw layer - the native
  files and the raw index `payload`, which is masked for everyone without `pii-reader`. A source whose
  clean layer must keep a CPR column for a legal purpose declares it with `cpr: true` (classified
  `direct` or `sensitive`, masked). The dimension carries `birth_year` instead of the birth date.
* Pseudonymised data is still personal data. The keys are classified `indirect` and deleted on erasure.
* Pepper rotation is **not supported yet**. It changes every key, and the history in clean,
  consolidated and curated (beyond the 90 days the raw index keeps) cannot be rebuilt: re-indexing
  and a full refresh would lose it, and without a full refresh every person would exist twice,
  under the old and the new pseudonym. A rotation needs its own migration that re-keys the stored
  history in place (old key -> new key per person, keystore business keys included). Re-indexing
  (`lakehouse-ingest index <source> --load-id <id> --reindex`) and full refresh are dev-only.
* The synthetic demo data cannot collide with real persons: the day part of every generated CPR
  number is offset by 60 (61-88), names are random combinations, e-mail uses the reserved `.example`
  TLD.

## Retention

`governance/retention.yml` holds the layer policy; table rules sit with the model:

| Object | Rule (demo values) |
|---|---|
| raw native files | 30 days after landing; files of aborted loads after 2 days |
| raw index rows | 90 days after indexing |
| `clean.borgerportal.payments`, `curated.borgerservice.fact_payment` | 5 years after payment / booking date |
| `clean.borgerportal.cases` | 5 years after closing; open cases stay |
| master data (`citizens`, `person`, `dim_citizen`) | kept while the person is registered; one year after deregistration (latest state `is_deleted`, meta `gdpr.deregistration`) the retention job queues the person for a *deregistration* erasure (below) |
| Iceberg snapshots (time travel) | raw 3 days, clean/consolidated 7 days, curated 14 days |

The `retention` job deletes expired rows and files (`lakehouse-retention --dry-run` reports first)
and queues deregistered persons for erasure.

**Deregistered persons (controller decision).** One year after deregistration the person's
identifiers are removed at once, but records that have their own retention duty - the payments
(bookkeeping) and cases, both 5 years - are not deleted early. The retention job queues such persons
with `scope = 'deregistration'`; the erasure job then treats every `erasure: delete` table that has
its own time rule (`retention_days` + `retention_column`) differently from an art. 17 request:

| Table kind | Art. 17 request (`scope = 'full'`) | Deregistration (`scope = 'deregistration'`) |
|---|---|---|
| raw index rows, native raw files | deleted | deleted |
| master data without time rule (`clean.citizens`, `consolidated.person`) | deleted | deleted |
| `erasure: delete` with own retention (`clean.payments`, `clean.cases`) | deleted | rows already past their retention are deleted; the others stay with the person key (`citizen_id`) and `anonymise_columns` (payments: `case_id`) set to NULL and `_anonymised_at` set |
| facts (`erasure: anonymise`) | kept, pointing at the erased member | same |
| dimensions + keystore | erased member | same |

The kept rows can no longer be linked to the person inside the lakehouse: the pseudonym is gone from
every table, the keystore no longer knows the business key, raw files and index rows are deleted.
They age out with the table's own rule (payments 5 years after the payment date, cases 5 years
after closing) through the normal `retention` job. Residual risks, accepted by the controller:
the case number (`clean.cases.case_id`, the table's key) still identifies the case in the source
system for as long as the source keeps it; a case that is still open never reaches its retention
rule and stays anonymised until it is closed in the source. The erasure audit row records the scope.

Deleted rows remain reachable through older snapshots until `iceberg_maintenance` expires them,
and - on versioned buckets - deleted files (expired Iceberg files, landing files removed by
retention) remain as noncurrent object versions for the **deletion safety window**:
`NONCURRENT_VERSION_GRACE_DAYS` (default 7, equal to the platform's bucket lifecycle
`lakehouse_noncurrent_version_days`). Within it an accidental delete can be undone by version id;
afterwards the lifecycle, or the maintenance step `purge_noncurrent_versions` (window or the layer's
orphan file retention, whichever is longer), removes them. Retention itself deletes only the current
version. The effective retention is therefore the rule plus the snapshot window plus the safety
window (raw 3+7 days, clean and consolidated 7+7, curated 14+7 with the demo values). Only the
erasure removes versions earlier, see below. The periods above are placeholders
for the demo; the controller sets the real ones.

## Right to erasure (GDPR art. 17)

```
lakehouse-erase --dry-run [--scope deregistration]          # tables and actions, no identifier
ERASURE_IDENTIFIER=<cpr> lakehouse-erase --requested-by dpo@... --reason "case 4711"
```

A request is queued: only the pseudonymised key is stored, in `raw.governance.erasure_requests`
(written with pyiceberg, readable by auditors and the pipeline only); the Dagster job `erasure`
executes the queue at night under the writer pool. Outside dev that is the only way: the CLI only
queues (an erasure outside the writer pool could race a dbt MERGE, whose uncommitted files the
zero-retention purge would delete). In dev the CLI executes at once unless `--submit`.
The dry run (`make erase-dry-run`) lists the tables and actions of the same plan the job executes
(per scope); it needs no identifier, runs no query and writes nothing. The real row counts are in
the audit row of the executed request. The identifier is read from the environment or stdin, never
from the command line, and never enters Dagster run configuration or events.

**No identifier in SQL.** Trino query text is archived (WORM) by the platform, so anything in it can
never be erased. Every erasure statement references the subject only as
`(select subject_key from raw.governance.erasure_requests where request_id = '<uuid>')`; the key, the
CPR number and the pepper stay in Python. The integration test checks the query history for this.
The one 64-hex value in query text is the keyed audit digest (step 6), which identifies nobody
without the pepper; the platform audit sink redacts 64-hex literals anyway.

What the nightly job does with the queue, as **one batch** (`governance/erasure/executor.py`; the plan
is derived from dbt meta and the source specs in `governance/erasure/plan.py`):

1. Stale dbt work tables (`__dbt_tmp`, `__stage`, `__dbt_backup`, left by failed runs) are dropped.
2. Per request: the native raw files containing the subject are located through the raw index;
   then
   * `raw.<source>.<entity>`: the subject's rows are deleted;
   * facts with `erasure: anonymise` keep their rows (e.g. bookkeeping duty) but `anonymise_columns`
     are set to NULL; they keep pointing at the member's surrogate keys;
   * clean models with `erasure: delete`: the subject's rows are deleted (scope `deregistration`:
     see the table above);
   * dimensions with `erasure: keystore`: the member's rows are replaced by anonymous placeholders
     (`is_erased`), and the keystore entries lose the business key (`erased:<key>`, type `erased`).
     The integer keys remain, are never reused, and carry no information about the person;
   * the native raw files are deleted including all object versions; a tombstone
     `<file>.erased.json` (no personal data) explains the gap. Records of other persons from those
     files remain available in the raw index.
3. Once per touched table for the whole batch (`governance/iceberg.py: purge_history`): `optimize`
   rewrites data files that carry row-level deletes; `optimize_manifests` rewrites the manifests,
   because the manifest entry of a removed file keeps that file's column min/max values (which can be
   the subject's key); a table that became empty is replaced by itself for the same reason;
   `expire_snapshots` and `remove_orphan_files` with zero retention then remove every older data
   file, delete file and manifest. Tens of requests cost one rewrite per table, not one per request.
4. Object versions: on a versioned bucket every file deleted in steps 1-3 is still readable by
   version id. The job removes every noncurrent version and delete marker **under the locations of
   the touched tables and of the dropped dbt work tables** (`PURGE_S3_*` credentials with
   `s3:DeleteObjectVersion`; the job fails rather than report success without the permission).
   Everywhere else the deletion safety window stays intact: the job runs every night, and a
   lake-wide purge would shrink the window for every accidental delete to less than a day. The
   price: copies of the subject in noncurrent versions *outside* the touched tables - typically a
   table that was dropped and re-created at a new location - stay readable by version id (only with
   read access to the bucket, not through the catalog) until the safety window expires them, at
   most `NONCURRENT_VERSION_GRACE_DAYS` after they became noncurrent. The controller treats them
   like backups (erasure completes when the window has passed).
5. Per request, verification in SQL: every planned table is queried again (through the queued key).
6. Per request, an audit row in `curated.audit.erasure_log`: erasure id, who requested it, reason,
   scope, status, executing principal, affected tables and row counts, and a keyed digest of the
   subject (lets the controller later prove that a given identifier was erased; identifies nobody by
   itself). The insert must report exactly one row; otherwise the erasure counts as failed.
7. Only now is the request removed from the queue. Finally the queue's own history and noncurrent
   object versions are purged as in steps 3-4 (every run, so a failed purge is repeated).

The integration test also scans every object version of the four buckets (Parquet, Avro manifests,
text) for the CPR number and the key after the erasure (`tests/integration/storage_scan.py`); that
O(lake) scan is a test, not part of the job.

**Failures never lose a request.** If any step fails, the request stays in the queue with
`status = 'failed'` and an incremented `attempts`; the key is kept, so the next run retries it
without the DPO resubmitting the identifier (every step is idempotent). A failed attempt is audited
as `failed`. A failing purge (step 3-4) fails every request of the batch. The nightly job processes
every queued request even when one fails, and then fails itself, listing the failed request ids (no
identifiers) - this is the operator's alert.

Limits and operating rules:

* Run exclusively. Steps 3-4 use a zero retention window; a concurrent writer could lose uncommitted
  files. The Dagster pool `lakehouse_writer` (limit 1) guarantees this for scheduled work.
* Time travel is lost for the touched tables (that is the point).
* Deleting whole native files removes the raw copy of other persons' records in the same files
  before their normal raw retention ends. Their data stays in the raw index and downstream; only
  the ability to re-index those files is lost.
* If the source system delivers the person again later, the person is ingested again under new
  surrogate keys. Erasure must also be carried out in the source system; this tool covers the
  lakehouse.
* Outside the scope of this tool: object store replicas and backups (`platform-backup`), central
  logs (which contain no subject keys from this tooling, see above), and consumers' extracts. They need their own retention so that erased data ages out.
* Whether a fact may be kept in anonymised form (`erasure: anonymise`) or must be deleted
  (`erasure: delete`) is a per-table decision of the controller, expressed in the model's meta.
  A `delete` table with its own time rule must have an `_anonymised_at` column (`make lint`
  enforces it): deregistration erasures keep its rows anonymised until the rule expires.
