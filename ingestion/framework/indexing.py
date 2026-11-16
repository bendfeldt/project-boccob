"""Stage 2 - INDEX: register landed records in ``raw.<source>.<entity>`` (schema-on-read).

One row per source record::

    payload        varchar      record rendered as JSON text (see formats.py); no typing
    _raw_file      varchar      object key of the native file in the raw bucket
    _raw_row       bigint       1-based position of the record in that file
    _load_id       varchar      landing load id
    _loaded_at     timestamptz  when the record was indexed
    _raw_format    varchar      native format of the file
    _raw_sha256    varchar      checksum of the native file (ties the row to the manifest)
    _subject_keys  varchar      JSON map pseudonym-name -> HMAC of the person identifier, or null

The table is an Iceberg table written by dlt's filesystem destination (``table_format=iceberg``)
through the Polaris REST catalog, so Trino sees it as ``raw.<source>.<entity>``.

Idempotency: a per-entity high-water mark of indexed load ids lives in dlt state, which dlt commits
only when the whole package has loaded. Before rows of a load are written, any rows already present
for that load id are deleted (left-overs of a partially loaded, failed attempt), so retries converge
instead of duplicating. ``reindex=True`` rewrites the requested load (e.g. after pepper rotation).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime

import dlt
import pyarrow as pa
from dlt.destinations.adapters import iceberg_adapter, iceberg_partition

from ingestion.framework import dlt_support
from ingestion.framework.formats import extract_field, iter_records
from ingestion.framework.manifest import ManifestEntry, read_manifests, sha256_hex
from ingestion.framework.pseudonym import pseudonymise
from ingestion.framework.spec import EntitySpec, SourceSpec
from lakehouse.settings import PolarisSettings, S3Settings, pseudonym_pepper

log = logging.getLogger(__name__)

INDEX_SCHEMA = pa.schema(
    [
        pa.field("payload", pa.string(), nullable=False),
        pa.field("_raw_file", pa.string(), nullable=False),
        pa.field("_raw_row", pa.int64(), nullable=False),
        pa.field("_load_id", pa.string(), nullable=False),
        pa.field("_loaded_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("_raw_format", pa.string(), nullable=False),
        pa.field("_raw_sha256", pa.string(), nullable=False),
        pa.field("_subject_keys", pa.string(), nullable=True),
    ]
)
BATCH_ROWS = 50_000
ERASED_SUFFIX = ".erased.json"  # tombstone written by governance.erasure


class ChecksumMismatchError(RuntimeError):
    pass


@dataclass
class IndexResult:
    source: str
    rows: dict[str, int] = field(default_factory=dict)  # entity -> rows indexed in this run
    load_ids: list[str] = field(default_factory=list)

    def summary(self) -> dict:
        return {"source": self.source, "load_ids": self.load_ids, "rows": self.rows}


def subject_keys(record_json: str, entity: EntitySpec, pepper: str | None) -> str | None:
    if not entity.identifiers:
        return None
    if pepper is None:
        raise RuntimeError(f"entity {entity.name} declares identifiers but no pepper is configured")
    keys = {}
    for name, path in entity.identifiers.items():
        value = extract_field(record_json, path)
        if value is not None:
            keys[name] = pseudonymise(value, pepper)
    return json.dumps(keys, sort_keys=True) if keys else None


def records_table(entry: ManifestEntry, entity: EntitySpec, content: bytes, pepper: str | None,
                  loaded_at: datetime) -> Iterator[pa.Table]:
    """Render one native file into index rows (pure function of file bytes + config)."""
    if sha256_hex(content) != entry.sha256:
        raise ChecksumMismatchError(f"{entry.path}: content does not match the manifest checksum")
    batch: list[dict] = []

    def flush() -> pa.Table:
        table = pa.Table.from_pylist(batch, schema=INDEX_SCHEMA)
        batch.clear()
        return table

    for row_no, record in enumerate(iter_records(entity.format, content, entity.options), start=1):
        batch.append(
            {
                "payload": record,
                "_raw_file": entry.path,
                "_raw_row": row_no,
                "_load_id": entry.load_id,
                "_loaded_at": loaded_at,
                "_raw_format": entity.format,
                "_raw_sha256": entry.sha256,
                "_subject_keys": subject_keys(record, entity, pepper),
            }
        )
        if len(batch) >= BATCH_ROWS:
            yield flush()
    if batch:
        yield flush()


def index_source(source: SourceSpec, *, load_id: str | None = None, reindex: bool = False,
                 s3: S3Settings | None = None, polaris: PolarisSettings | None = None) -> IndexResult:
    """Index committed loads of ``source`` that are not indexed yet (or exactly ``load_id``)."""
    dlt_support.configure_runtime()
    s3 = s3 or S3Settings.from_env()
    polaris = polaris or PolarisSettings.from_env()
    fs = s3.filesystem()
    needs_pepper = any(e.identifiers for e in source.entities)
    pepper = pseudonym_pepper() if needs_pepper else None
    loads = read_manifests(fs, s3.raw_bucket, source.name, load_id or "*")
    if load_id and load_id not in loads:
        raise LookupError(f"no committed load {load_id} for source {source.name}")
    if reindex and not load_id:
        raise ValueError("reindex requires an explicit load_id")
    result = IndexResult(source=source.name)
    loaded_at = datetime.now(UTC)
    touched: set[str] = set()
    catalogs: list = []

    def catalog():
        if not catalogs:
            from pyiceberg.catalog import load_catalog

            catalogs.append(load_catalog(polaris.raw_catalog, **polaris.catalog_properties(polaris.raw_catalog, s3)))
        return catalogs[0]

    def make_resource(entity: EntitySpec):
        @dlt.resource(name=entity.name, write_disposition="append", table_format="iceberg")
        def entity_records():
            state = dlt.current.resource_state()
            high_water = float(state.get("last_indexed_load_id", "0"))
            for lid, entries in loads.items():
                if not reindex and float(lid) <= high_water:
                    continue
                mine = [e for e in entries if e.entity == entity.name]
                if mine:
                    _delete_load(catalog(), source, entity, lid)
                for entry in mine:
                    target = f"{s3.raw_bucket}/{entry.path}"
                    if not fs.exists(target) and fs.exists(target + ERASED_SUFFIX):
                        log.warning("skipping %s: removed by a right-to-erasure request", entry.path)
                        continue
                    content = fs.cat_file(target)
                    for table in records_table(entry, entity, content, pepper, loaded_at):
                        result.rows[entity.name] = result.rows.get(entity.name, 0) + table.num_rows
                        yield table
                touched.add(lid)
                if float(lid) > high_water:
                    high_water = float(lid)
                    state["last_indexed_load_id"] = lid

        return iceberg_adapter(entity_records, partition=[iceberg_partition.day("_loaded_at")])

    pipeline = dlt.pipeline(
        pipeline_name=f"index_{source.name}",
        destination=dlt_support.index_destination(s3, polaris),
        dataset_name=source.name,
        pipelines_dir=dlt_support.pipelines_dir(),
    )
    info = pipeline.run([make_resource(e) for e in source.entities])
    info.raise_on_failed_jobs()
    result.load_ids = sorted(touched, key=float)
    log.info("indexed %s", result.summary())
    return result


def _delete_load(catalog, source: SourceSpec, entity: EntitySpec, load_id: str) -> None:
    """Remove rows of ``load_id`` (no-op for a load that was never written)."""
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
    from pyiceberg.expressions import EqualTo

    try:
        table = catalog.load_table(f"{source.name}.{entity.name}")
    except (NoSuchTableError, NoSuchNamespaceError):
        return
    table.delete(EqualTo("_load_id", load_id))
