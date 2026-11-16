# Dimensional keystore

Every dimension gets its surrogate keys from a keystore table in `curated.keystore`. The keystore is
the only stateful part of a dimension: the dimension table is a projection of its source and can be
dropped and rebuilt at any time without a single key changing.

Code: `transform/macros/keystore/keystore.sql`, `transform/macros/materializations/dimension.sql`,
tests in `transform/macros/tests/generic_tests.sql`.

## Table layout

One Iceberg table per dimension, `curated.keystore.<dimension model name>`:

| Column | Meaning |
|---|---|
| `surrogate_key` | sequential integer (`bigint`, var `keystore_key_type`), starts at 1, `-1` = unknown member |
| `business_key` | business key as text (pseudonymised when it identifies a person) |
| `version_key` | SCD2: effective-from of the version as UTC text; SCD1: empty string |
| `member_type` | `regular`, `inferred`, `unknown`, `erased` |
| `key_created_at`, `key_created_by` | when the key was allocated and by which dbt invocation/attempt |
| `key_last_used_in_fact_at`, `key_last_used_by` | last fact load that referenced the key, and which fact |
| `adopted_at` | when an inferred member received its master data |

Rules: a row is only ever inserted; `surrogate_key` and `key_created_at` are never updated; a key is
never deleted and never handed out twice (also not after an erasure).

## Using it

A dimension is one model:

```sql
{{ config(materialized='dimension', business_key='person_id', effective_from='valid_from',
          surrogate_key='citizen_key') }}          -- SCD2 is the default
select person_id, valid_from, full_name, postal_code, ... from {{ ref('person') }}
```

`scd_type=1` switches a dimension to SCD1 (`dim_case_type`); the project default is the var
`scd_default_type`. The model returns change records (any number of rows per business key); the
materialization

1. stages the query and refuses ambiguous input (several rows per business key and effective-from),
2. collapses consecutive records with identical attributes into one version (SCD2) or keeps the
   newest record (SCD1),
3. allocates keys for (business key, version) pairs that have none - existing ones are reused,
4. rebuilds the table with `CREATE OR REPLACE TABLE` (atomic), adding the unknown member (`-1`),
   inferred members and erased members from the keystore.

Generated columns: the surrogate key, `is_inferred`, `is_erased`, and for SCD2 `valid_from`
(inclusive; the first version starts at 1900-01-01), `valid_to` (exclusive; 9999-12-31 for the
current version) and `is_current`. The business key must be a varchar.

A fact model uses three helpers:

```sql
{{ config(materialized='incremental', unique_key='payment_id', incremental_strategy='merge',
    pre_hook=["{{ keystore_register_inferred('dim_citizen', ref('payments'), 'citizen_id') }}"],
    post_hook=["{{ keystore_mark_used('dim_citizen', 'citizen_key') }}"]) }}
select p.payment_id, {{ dim_key('dim_citizen', 'ci') }} as citizen_key, ..., {{ audit_columns() }}
from {{ ref('payments') }} p
{{ dim_join('dim_citizen', 'ci', 'p.citizen_id', at='p.payment_date') }}
```

* `dim_join(..., at=...)` joins the version valid at the event time, so late-arriving facts get the
  historically correct version; without `at` the current version is used.
* `dim_key` maps a missing business key to `-1`.
* `keystore_register_inferred` (pre-hook) handles late-arriving dimension members: a business key
  that occurs in the fact source but has no key yet gets an *inferred* key and a placeholder row in
  the dimension, so the fact is loaded now. When the master data arrives, the earliest version
  adopts that key (`member_type` becomes `regular`, `adopted_at` is set) - the fact rows never need
  to be touched.
* `keystore_mark_used` (post-hook) stamps `key_last_used_in_fact_at` for every key referenced by the
  rows written in this invocation (`_invocation_id` from `audit_columns()`).

## Allocation strategy and its limits

Trino on Iceberg has no sequences and no locks, and Iceberg does not detect conflicts between two
concurrent appends. Allocation is therefore **optimistic and verified**:

1. One `INSERT ... SELECT max(surrogate_key) + row_number()` adds all missing candidates. It is a
   single atomic Iceberg commit: other readers see either none or all of the new keys.
2. Immediately afterwards the keystore is checked for duplicate surrogate keys and duplicate
   (business key, version) pairs.
3. If a concurrent writer caused a collision, the rows of *this attempt* (identified by
   `key_created_by = <invocation>:<dimension>:<type>:<attempt>`) are deleted and the allocation is
   retried, up to `keystore_max_attempts` (3). Keys that existed before the attempt are never touched.
   If it still fails the model fails; nothing has been built on the withdrawn keys because the
   dimension is only rebuilt after a clean allocation.

Limits, stated plainly:

* This is detection and compensation, not mutual exclusion. Between step 1 and step 3 of a colliding
  attempt another session could read a duplicate key from the keystore. The dimension tables are
  never exposed to that state by the same run, but an independent reader of the keystore could be.
* If both colliding writers detect the collision, both withdraw and retry; with unlucky timing all
  attempts can collide and both runs fail (safe, but not live).
* The design therefore relies on a **single writer per keystore table**: one dbt model owns each
  dimension, a fact's pre-hook is the only other writer, dbt orders them through `ref`, and all
  Dagster assets/ops that write use the pool `lakehouse_writer` (limit 1, see
  docs/platform-contract.md). Verification and retry are the safety net for operator mistakes
  (two manual runs at once), not the primary mechanism.
* Concurrent allocation was not load-tested in the integration harness.
* Keys are dense as long as no attempt is withdrawn; a withdrawn attempt does not leave gaps either
  (its rows are removed before the next `max()`), but an erased member keeps its number forever.
* Changing a dimension from SCD2 to SCD1 (or back) changes the version key, so new keys are
  allocated; the old ones stay in the keystore, unused. Treat it as a new dimension.
* If history is rewritten upstream so that a version's effective-from changes, that version is a new
  (business key, version) pair and gets a new key; existing fact rows keep the old key.

## Tests

dbt (run with every `dbt build`, surfaced as Dagster asset checks):

| Test | Proves |
|---|---|
| `keystore_integrity` | no duplicate surrogate keys, no duplicate (business key, version), every dimension row carries exactly the keystore's key, creation timestamps set, unknown member present |
| `keystore_stable_since_baseline` | every key recorded by `dbt run-operation keystore_snapshot_baseline` still belongs to the same business key and version (erased members excepted) |
| `scd2_no_overlap`, `scd2_contiguous_one_current` | validity intervals do not overlap, have no gaps, exactly one current version |
| `keystore_usage` | every key a fact references exists in the dimension and has `key_last_used_in_fact_at >= key_created_at` |

Integration (`tests/integration/test_e2e.py`, on Trino/Polaris/RustFS): keys and creation timestamps
are identical after a changed load, after dropping both dimension tables and after
`dbt build --full-refresh` of everything; facts resolve to the same keys after the full refresh;
only keys referenced by the newest fact load get a new last-used timestamp; inferred members keep
their key when adopted; keys are sequential without gaps; erased keys are never reassigned.
