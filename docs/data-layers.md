# Data layers

```
source system
   | dlt  LAND   (ingestion/framework/landing.py)
   v
raw      s3://lakehouse-raw/<source>/<entity>/load_date=YYYY-MM-DD/<load_id>/<native file>
   | dlt  INDEX  (ingestion/framework/indexing.py)
   v
raw      raw.<source>.<entity>            schema-on-read Iceberg index
   | dbt
   v
clean    clean.<source>.<entity>          typed, deduplicated, denormalised, pseudonymised
   | dbt                       \
   v                            \  (the consolidated layer is optional)
consolidated  consolidated.<domain>.<entity>   business entities
   | dbt                        /
   v                           v
curated  curated.<domain>.dim_* / fact_* / dp_*   + curated.keystore.<dimension>
```

One Polaris catalog and one Trino catalog per layer (`raw`, `clean`, `consolidated`, `curated`), one
bucket per layer. Catalog names can be overridden with `RAW_CATALOG`, `CLEAN_CATALOG`,
`CONSOLIDATED_CATALOG`, `CURATED_CATALOG`.

## raw - landing area, source aligned, no structural assumptions

Two parts, written by two dlt pipelines per source.

**Native files (LAND).** Whatever the source delivers is stored byte for byte:

```
s3://lakehouse-raw/<source>/<entity>/load_date=YYYY-MM-DD/<load_id>/<file as delivered>
s3://lakehouse-raw/<source>/_manifest/<load_id>.<n>.jsonl      one row per file: size, sha256, record count
s3://lakehouse-raw/<source>/_dlt_loads | _dlt_pipeline_state | _dlt_version      dlt bookkeeping
```

* `load_id` is the dlt load id, `load_date` its UTC date. Objects are never overwritten (the
  pipeline refuses to write an existing key).
* The manifest is written last. A load is committed only when its manifest exists; files of an
  aborted load are ignored by the index stage and removed by the retention job.
* Extraction cursors live in dlt pipeline state, stored in the bucket, so pods are stateless.
* dlt bookkeeping holds no personal data: dlt keeps only the newest pipeline state files and writes
  a schema file per schema change; its load records (`_dlt_loads`, one per load, of the landing and
  the index pipeline) are removed by the retention job with the native files (30 days).

**Index (INDEX).** `raw.<source>.<entity>` makes the records addressable from SQL:

| Column | Content |
|---|---|
| `payload` | one source record as JSON text - JSON as is, CSV row as `{header: text}`, XML element as object with `@attributes`, parquet row as object. No typing, no renaming |
| `_raw_file`, `_raw_row` | native file (object key) and position of the record in it |
| `_load_id`, `_loaded_at` | landing load and time of indexing |
| `_raw_format`, `_raw_sha256` | native format and file checksum (ties the row to the manifest) |
| `_subject_keys` | JSON map of pseudonymised person identifiers found in the record (see docs/gdpr.md) |

The table lives at `s3://lakehouse-raw/iceberg/<source>/<entity>` and is written by dlt's filesystem
destination in Iceberg format through the Polaris REST catalog. Indexing is idempotent: the set of
indexed load ids in dlt state decides what is pending (a set, so overlapping landings that commit
out of order lose nothing), and rows of a load are deleted before they are written
again, so a retried or repeated run never duplicates.

Why dlt is used this way (checked against dlt 1.32):

* File import (`dlt.mark.with_file_import`) copies bytes untouched, but only for dlt's own loader
  formats and with generated names, so an XML or JSON-document file would land as `<hash>.csv`.
  Native files are therefore uploaded by the landing resource itself, while dlt provides the load id,
  the state, the manifest table and the load bookkeeping.
* The filesystem destination with `table_format="iceberg"` supports REST catalogs. It places a
  table at `<bucket_url>/<dataset>/<table>`, which Polaris accepts only inside the catalog's base
  location - hence `RAW_INDEX_PREFIX=iceberg`.

## clean - source aligned, structured

`transform/models/clean/<source>/`. One model per entity, incremental (Iceberg `MERGE`) on the raw
index, loading only rows indexed since the previous run.

* parse and type (`json_extract_scalar`, Danish decimal and date formats, ISO timestamps)
* deduplicate on the natural key (newest load wins)
* denormalise (address into the citizen row, case type into the payment row - also when the case
  arrives after the payment: the payment row is merged again, and the fact re-keys it)
* person identifiers replaced by the pseudonymised key from `_subject_keys`; the CPR number itself
  is not carried into clean (it stays in the raw layer only)
* data quality tests: not null, uniqueness, accepted values, relationships, pseudonym format
* lineage columns `_raw_file`, `_raw_row`, `_load_id`, `_loaded_at`, `_invocation_id`

`citizens` keeps every delivered state (one row per citizen and change timestamp); that history is
what makes SCD2 dimensions rebuildable.

## consolidated - business entities (optional)

`transform/models/consolidated/<domain>/`. `consolidated.party.person` presents the person entity in
business vocabulary, independent of the delivering system; further source systems are added as
`UNION ALL` branches (the pseudonymised key is identical for the same CPR number in every source).

The layer is optional per data flow:

* `dim_citizen` reads `consolidated.party.person`
* `dim_case_type` and `fact_payment` read `clean` directly - reference data and single-source facts
  need no consolidation

## curated - dimensional model and data products

`transform/models/curated/<domain>/`.

| Model | Kind | Notes |
|---|---|---|
| `dim_citizen` | dimension, SCD2 (default) | keys from `curated.keystore.dim_citizen` |
| `dim_case_type` | dimension, SCD1 (`scd_type=1`) | keys from `curated.keystore.dim_case_type` |
| `dim_date` | calendar | key `yyyymmdd`, deterministic, not in the keystore |
| `fact_payment` | fact, incremental | version valid at payment date, inferred members, `-1` unknown |
| `dp_payment_summary` (v1) | data product | enforced dbt contract, `access: public`, group, owner/SLA meta, exposure `payment_summary_dashboard`, small-cell flag with OPA row filter |

See docs/keystore.md for the keystore.

## Orchestration and lineage

Dagster asset keys follow the same addressing: `landing/<source>/<entity>` -> `raw/<source>/<entity>`
-> `clean/...` -> `consolidated/...` -> `curated/...`. The raw asset keys are the dbt source keys, so
the lineage graph is continuous from the native file to the data product. dbt tests appear as asset
checks on the model they test.
