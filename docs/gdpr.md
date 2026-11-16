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
| CPR confined to one column | `clean.<source>.citizens.cpr_number` | integration test (no CPR column or value in consolidated/curated) |
| Retention | `governance/retention.yml` + dbt `meta.gdpr` | Dagster job `retention` |
| Right to erasure | `governance/erasure` | CLI `lakehouse-erase`, Dagster job `erasure`, audit table |
| Disclosure control for published aggregates | `dp_payment_summary.is_small_cell` + row filter | Trino OPA plugin (row filters) |

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
| `platform-admin` | rw | rw | rw | rw | no automatic PII visibility; may kill queries, read system information |
| `data-engineer` | rw | rw | rw | rw | incl. table maintenance procedures |
| `analyst` | - | - | - | read | not `curated.keystore`, not `curated.audit` |
| `auditor` | read | read | read | read | read-only, sees all users' queries (`system.runtime`) |
| `pii-reader` | | | | | modifier only: removes column masks and row filters marked exempt; grants no access by itself |

* Everything not explicitly allowed is denied; users without one of these groups cannot run queries.
  Impersonation, catalog management and authorization changes are denied for everyone.
* Masks: `redact` -> `'*****'`, `email` -> local part replaced, `hash` -> SHA-256, `null` and every
  non-text column -> `NULL`. In `raw`, `payload` is masked for everyone without `pii-reader`
  (the record text cannot be masked field by field).
* Pipeline service accounts must be in `data-engineer` **and** `pii-reader`; a masked pipeline would
  write masked values downstream.
* 36 policy unit tests (`make opa-test`); the same rules are exercised through a real Trino in the
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
  number exists in exactly one governed column, `clean.borgerportal.citizens.cpr_number`, for the
  purposes that legally require it; it is classified `direct` + `cpr`, masked, and not propagated to
  consolidated or curated. The dimension carries `birth_year` instead of the birth date.
* Pseudonymised data is still personal data. The keys are classified `indirect` and deleted on erasure.
* Pepper rotation changes every key. Procedure: set the new pepper, re-index all retained loads
  (`lakehouse-ingest index <source> --load-id <id> --reindex`), full-refresh dbt. Surrogate keys of
  person dimensions are then allocated anew (the business key changed), so plan a rotation like a
  migration. Loads whose raw files have passed retention cannot be re-indexed.
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
| master data (`citizens`, `person`, `dim_citizen`) | no time rule: kept while the person is registered, removed by erasure. The retention report lists these tables under `no_time_rule` so the gap is visible |
| Iceberg snapshots (time travel) | raw 3 days, clean/consolidated 7 days, curated 14 days |

The `retention` job deletes expired rows and files (`lakehouse-retention --dry-run` reports first).
Deleted rows remain reachable through older snapshots until `iceberg_maintenance` expires them, so
the effective retention is the rule plus the snapshot window. The periods above are placeholders
for the demo; the controller sets the real ones.

## Right to erasure (GDPR art. 17)

```
ERASURE_IDENTIFIER=<cpr> lakehouse-erase --requested-by dpo@... --reason "case 4711" --dry-run
ERASURE_IDENTIFIER=<cpr> lakehouse-erase --requested-by dpo@... --reason "case 4711" --submit
```

`--submit` queues the request (only the pseudonymised key is stored, in `curated.audit`); the
Dagster job `erasure` executes the queue at night under the writer pool. Without `--submit` the
erasure runs immediately. The identifier is read from the environment or stdin, never from the
command line, and never enters Dagster run configuration or events.

What happens, in order (`governance/erasure/plan.py`, derived from dbt meta and the source specs):

1. The identifier is turned into the pseudonymised key.
2. The native raw files containing the subject are located through the raw index.
3. `raw.<source>.<entity>`: the subject's rows are deleted.
4. Facts with `erasure: anonymise` keep their rows (e.g. bookkeeping duty) but `anonymise_columns`
   are set to NULL; they keep pointing at the member's surrogate keys.
5. clean and consolidated models with `erasure: delete`: the subject's rows are deleted.
6. Dimensions with `erasure: keystore`: the member's rows are replaced by anonymous placeholders
   (`is_erased`), and the keystore entries lose the business key (`erased:<key>`, type `erased`).
   The integer keys remain, are never reused, and carry no information about the person.
7. The native raw files are deleted including all object versions; a tombstone
   `<file>.erased.json` (no personal data) explains the gap. Records of other persons from those
   files remain available in the raw index.
8. Every touched table is purged (`governance/iceberg.py: purge_history`): `optimize` rewrites data
   files that carry row-level deletes; `optimize_manifests` rewrites the manifests, because the
   manifest entry of a removed file keeps that file's column min/max values (which can be the
   subject's key or CPR number); a table that became empty is replaced by itself for the same
   reason; `expire_snapshots` and `remove_orphan_files` with zero retention then remove every
   older data file, delete file and manifest.
9. Verification: all tables are queried again; with `--verify-storage` / `ERASURE_VERIFY_STORAGE`
   every object in the four layer buckets is decoded and searched for the identifier and the key.
10. An audit row is written to `curated.audit.erasure_log`: erasure id, who requested it, reason,
    status, executing principal, affected tables and counts, and a keyed digest of the subject
    (lets the controller later prove that a given identifier was erased; identifies nobody by itself).

Limits and operating rules:

* Run exclusively. Step 8 uses a zero retention window; a concurrent writer could lose uncommitted
  files. The Dagster pool `lakehouse_writer` (limit 1) guarantees this for scheduled work.
* Time travel is lost for the touched tables (that is the point).
* Deleting whole native files removes the raw copy of other persons' records in the same files
  before their normal raw retention ends. Their data stays in the raw index and downstream; only
  the ability to re-index those files is lost.
* If the source system delivers the person again later, the person is ingested again under new
  surrogate keys. Erasure must also be carried out in the source system; this tool covers the
  lakehouse.
* Outside the scope of this tool: object store replicas and backups (`platform-backup`), central
  logs, and consumers' extracts. They need their own retention so that erased data ages out.
* Whether a fact may be kept in anonymised form (`erasure: anonymise`) or must be deleted
  (`erasure: delete`) is a per-table decision of the controller, expressed in the model's meta.
