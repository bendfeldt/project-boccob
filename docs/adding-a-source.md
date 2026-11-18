# Adding a source

A new source is a directory with one YAML file and one extractor function, plus dbt models for the
layers you want. No framework code changes; Dagster picks the source up automatically.

## 1. Describe the source

`ingestion/sources/<source>/source.yml`:

```yaml
name: hr_system                       # [a-z][a-z0-9_]*, becomes raw.<name> and the bucket prefix
description: Personnel system export
owner: team-hr
extractor: ingestion.sources.hr_system.extract:extract
options:                              # non-secret settings, override with INGEST_HR_SYSTEM_<OPTION>
  base_url: https://hr.example.internal/api
  page_size: 500
entities:
  - name: employees
    format: json                      # json | jsonl | csv | xml | parquet
    options: {record_path: items}     # json: where the list of records is; csv: delimiter, encoding; xml: record_tag
    identifiers: {cpr: person.cpr}    # person identifiers to pseudonymise: <key name>: <field path in a record>
  - name: departments
    format: csv
    options: {delimiter: ";"}
```

`identifiers` is the only thing the index stage needs to know about the content. Use the same key
name (`cpr`) for the same kind of identifier in every source: the pseudonym is then identical across
sources, which is what the consolidated layer joins on.

The name `iceberg` is reserved (prefix of the raw index tables).

## 2. Write the extractor

`ingestion/sources/<source>/extract.py` (and an empty `__init__.py`):

```python
import dlt
from ingestion.framework.spec import ExtractContext, NativeFile


def extract(ctx: ExtractContext):
    token = dlt.secrets["sources.hr_system.api_token"]  # env: SOURCES__HR_SYSTEM__API_TOKEN
    since = ctx.state.get("cursor", "1970-01-01T00:00:00Z")
    body, newest = download_employees(ctx.options["base_url"], token, since)  # bytes exactly as delivered
    yield NativeFile("employees", f"employees_{ctx.logical_date}.json", body)
    yield NativeFile("departments", "departments.csv", download_departments(...))
    ctx.state["cursor"] = newest  # committed by dlt only when the whole load succeeded
```

Rules:

* Yield the bytes exactly as the source delivered them. Do not parse, re-encode or filter.
* Keep cursors in `ctx.state`; it is stored with the load in the raw bucket, pods are stateless.
* Read credentials from `dlt.secrets` / the environment. Nothing secret in `source.yml`.
* File names must be unique within one load and contain no `/`.
* Yield nothing when the source has no new data; no load is committed then.

Try it: `scripts/run.sh lakehouse-ingest list`, then `scripts/run.sh lakehouse-ingest run hr_system`.
The files appear under `s3://lakehouse-raw/hr_system/<entity>/load_date=.../<load_id>/` and the
records in `raw.hr_system.<entity>`.

## 3. Model it in dbt

1. `transform/models/clean/hr_system/_sources.yml`: declare the dbt source `raw_hr_system` with
   `database: "{{ env_var('RAW_CATALOG', 'raw') }}"`, `schema: hr_system` and the entity tables
   (copy the Borgerportal file).
2. One clean model per entity, using the helpers in `macros/utils/raw_parsing.sql`:

   ```sql
   {{ config(materialized='incremental', incremental_strategy='merge', unique_key='employee_id',
             on_schema_change='append_new_columns') }}
   with raw as (
     select {{ raw_columns() }} from {{ source('raw_hr_system', 'employees') }} {{ raw_incremental_filter() }}
   )
   select {{ json_text('$.id') }} as employee_id,
          {{ subject_key('cpr') }} as person_id,      -- pseudonymised key, never the identifier itself
          ...,
          _raw_file, _raw_row, _load_id, _loaded_at, {{ audit_columns() }}
   from raw
   ```
3. Add the folder to `dbt_project.yml` (`+schema: hr_system` under `clean`).
4. Document every column with `config.meta.pii` and give person-bound models a `meta.gdpr` block
   (docs/gdpr.md). `make check-classification` tells you what is missing.
5. Optional: add a branch to a consolidated entity (e.g. `consolidated.party.person`), and
   dimensions/facts in curated (docs/keystore.md).
6. `make classification` to regenerate the OPA data, `make test`, `make lint`.
7. Incremental models use `on_schema_change='append_new_columns'`. Once a model is released, any
   non-additive change of it (drop, rename, retype) needs a migration - see
   [releases.md](releases.md); a full refresh is refused outside dev.

## 4. What you get without further work

* Dagster assets `landing/hr_system/<entity>` and `raw/hr_system/<entity>` with daily partitions, a
  job `ingest_hr_system`, lineage into the dbt models, inclusion in `daily_refresh`.
* Manifest with checksums for every load, idempotent indexing.
* Retention of the raw files and index rows, erasure across raw and every model with `meta.gdpr`.
* Column masks for classified columns as soon as the new OPA bundle is deployed.
