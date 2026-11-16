"""Stage 1 - LAND: copy source data, byte for byte, into the immutable raw bucket.

Layout (architecture contract)::

    s3://<raw-bucket>/<source>/<entity>/load_date=YYYY-MM-DD/<load_id>/<native file>
    s3://<raw-bucket>/<source>/_manifest/<load_id>.<n>.jsonl          <- commit marker + audit
    s3://<raw-bucket>/<source>/_dlt_loads|_dlt_pipeline_state|...     <- dlt bookkeeping

The dlt pipeline owns the load id, the extraction state (cursors) and the manifest table. Native
files are uploaded during extract and never rewritten; the manifest (one row per file with size
and SHA-256) is loaded last, so a load is *committed* only when its manifest exists. Files of an
aborted load have no manifest, are ignored by the index stage and removed by the retention job.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

import dlt

from ingestion.framework import dlt_support
from ingestion.framework.manifest import MANIFEST_TABLE, ManifestEntry, landing_key, load_date_of, sha256_hex
from ingestion.framework.spec import ExtractContext, SourceSpec, SpecError
from lakehouse.settings import S3Settings

log = logging.getLogger(__name__)

MANIFEST_COLUMNS = {
    "source": {"data_type": "text"},
    "entity": {"data_type": "text"},
    "load_id": {"data_type": "text"},
    "load_date": {"data_type": "text"},
    "logical_date": {"data_type": "text"},
    "file_name": {"data_type": "text"},
    "path": {"data_type": "text"},
    "format": {"data_type": "text"},
    "size_bytes": {"data_type": "bigint"},
    "sha256": {"data_type": "text"},
    "record_count": {"data_type": "bigint", "nullable": True},
    "landed_at": {"data_type": "text"},
}


class ImmutabilityError(RuntimeError):
    pass


@dataclass
class LandResult:
    source: str
    load_id: str | None
    files: list[ManifestEntry] = field(default_factory=list)

    @property
    def committed(self) -> bool:
        return self.load_id is not None and bool(self.files)

    def summary(self) -> dict:
        per_entity: dict[str, dict[str, int]] = {}
        for f in self.files:
            agg = per_entity.setdefault(f.entity, {"files": 0, "bytes": 0, "records": 0})
            agg["files"] += 1
            agg["bytes"] += f.size_bytes
            agg["records"] += f.record_count or 0
        return {"source": self.source, "load_id": self.load_id, "entities": per_entity}


def land_source(source: SourceSpec, *, logical_date: str | None = None, s3: S3Settings | None = None) -> LandResult:
    """Run one landing load for ``source`` and return what was committed."""
    dlt_support.configure_runtime()
    s3 = s3 or S3Settings.from_env()
    fs = s3.filesystem()
    extractor = source.load_extractor()
    logical = logical_date or datetime.now(UTC).strftime("%Y-%m-%d")
    landed: list[ManifestEntry] = []

    @dlt.resource(name=MANIFEST_TABLE, write_disposition="append", columns=MANIFEST_COLUMNS)
    def manifest():
        load_id = dlt.current.load_package_state()["load_id"]
        ctx = ExtractContext(
            source=source,
            load_id=load_id,
            logical_date=logical,
            state=dlt.current.resource_state(),
            options=source.resolved_options(),
        )
        for native in extractor(ctx):
            entity = source.entity(native.entity)  # raises on undeclared entities
            key = landing_key(source.name, entity.name, load_id, native.file_name)
            target = f"{s3.raw_bucket}/{key}"
            if fs.exists(target):
                raise ImmutabilityError(f"refusing to overwrite landed object s3://{target}")
            fs.pipe_file(target, native.content)
            entry = ManifestEntry(
                source=source.name,
                entity=entity.name,
                load_id=load_id,
                load_date=load_date_of(load_id),
                logical_date=logical,
                file_name=native.file_name,
                path=key,
                format=entity.format,
                size_bytes=len(native.content),
                sha256=sha256_hex(native.content),
                record_count=native.record_count,
                landed_at=datetime.now(UTC).isoformat(),
            )
            landed.append(entry)
            log.info("landed s3://%s (%d bytes, sha256=%s)", target, entry.size_bytes, entry.sha256)
            yield entry.as_row()

    pipeline = dlt.pipeline(
        pipeline_name=f"land_{source.name}",
        destination=dlt_support.landing_destination(s3),
        dataset_name=source.name,
        pipelines_dir=dlt_support.pipelines_dir(),
    )
    info = pipeline.run(manifest(), loader_file_format="jsonl")
    info.raise_on_failed_jobs()
    if not landed:
        log.info("source %s delivered no files; nothing committed", source.name)
        return LandResult(source=source.name, load_id=None)
    load_ids = {e.load_id for e in landed}
    if len(load_ids) != 1 or not set(info.loads_ids) >= load_ids:
        raise SpecError(f"inconsistent load ids: landed={load_ids}, dlt={info.loads_ids}")
    return LandResult(source=source.name, load_id=load_ids.pop(), files=landed)
