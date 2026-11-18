"""Scheduled Iceberg maintenance for every table of the lakehouse layers."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from governance import iceberg
from governance.config import RetentionConfig, catalog_of, load_retention
from lakehouse.objectstore import purge_noncurrent
from lakehouse.settings import LAYERS, S3Settings, layer_bucket, noncurrent_grace_days
from lakehouse.trino_client import TrinoClient

log = logging.getLogger(__name__)
OPERATIONS = ("optimize", "expire_snapshots", "remove_orphan_files", "purge_noncurrent_versions")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def duration_seconds(value: str) -> int:
    return int(value[:-1]) * _UNITS[value[-1]]


@dataclass
class MaintenanceReport:
    done: list[dict] = field(default_factory=list)
    failed: list[dict] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed


def run_maintenance(
    layers: tuple[str, ...] = LAYERS,
    operations: tuple[str, ...] = OPERATIONS,
    schema: str | None = None,
    config: RetentionConfig | None = None,
    trino: TrinoClient | None = None,
) -> MaintenanceReport:
    config = config or load_retention()
    unknown = set(operations) - set(OPERATIONS)
    if unknown:
        raise ValueError(f"unknown maintenance operations: {sorted(unknown)}")
    catalogs = [catalog_of(layer) for layer in layers]
    # floor lowered to 1h: retention thresholds below Trino's 7d default are a deliberate policy choice
    trino = trino or TrinoClient(
        session_properties=iceberg.retention_session(catalogs, "1h"), source="lakehouse-maintenance"
    )
    report = MaintenanceReport()
    for layer in layers:
        policy = config.layers[layer]
        for table in iceberg.list_tables(trino, catalog_of(layer), config.exclude_schemas, schema):
            for op in operations:
                if op == "purge_noncurrent_versions":
                    continue  # bucket level, below
                try:
                    if op == "optimize":
                        iceberg.optimize(trino, table, config.optimize_file_size_threshold)
                    elif op == "expire_snapshots":
                        iceberg.expire_snapshots(trino, table, policy.snapshot_retention)
                    else:
                        iceberg.remove_orphan_files(trino, table, policy.orphan_file_retention)
                    report.done.append({"table": str(table), "operation": op})
                except Exception as exc:  # keep going: one broken table must not block the rest
                    log.error("maintenance %s failed on %s: %s", op, table, exc)
                    report.failed.append({"table": str(table), "operation": op, "error": str(exc)[:500]})
    if "purge_noncurrent_versions" in operations and schema is None:
        # Files removed by expire_snapshots / remove_orphan_files / DROP TABLE stay readable as
        # noncurrent versions on a versioned bucket - the deletion safety window. They are removed
        # once noncurrent for longer than NONCURRENT_VERSION_GRACE_DAYS (default 7, same as the
        # bucket lifecycle), or the layer's orphan file retention if that is longer.
        purge_s3 = S3Settings.for_purge()
        fs = purge_s3.filesystem()
        grace = timedelta(days=noncurrent_grace_days())
        for layer in layers:
            bucket = purge_s3.raw_bucket if layer == "raw" else layer_bucket(layer)
            orphan = timedelta(seconds=duration_seconds(config.layers[layer].orphan_file_retention))
            cutoff = datetime.now(UTC) - max(grace, orphan)
            try:
                removed = purge_noncurrent(fs, bucket, older_than=cutoff)
                report.done.append(
                    {"table": f"s3://{bucket}", "operation": "purge_noncurrent_versions", "removed": removed}
                )
            except Exception as exc:
                log.error("purge of noncurrent versions failed in %s: %s", bucket, exc)
                report.failed.append(
                    {"table": f"s3://{bucket}", "operation": "purge_noncurrent_versions", "error": str(exc)[:500]}
                )
    log.info("maintenance finished: %d ok, %d failed", len(report.done), len(report.failed))
    return report
