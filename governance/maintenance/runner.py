"""Scheduled Iceberg maintenance for every table of the lakehouse layers."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from governance import iceberg
from governance.config import RetentionConfig, catalog_of, load_retention
from lakehouse.settings import LAYERS
from lakehouse.trino_client import TrinoClient

log = logging.getLogger(__name__)
OPERATIONS = ("optimize", "expire_snapshots", "remove_orphan_files")


@dataclass
class MaintenanceReport:
    done: list[dict] = field(default_factory=list)
    failed: list[dict] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed


def run_maintenance(layers: tuple[str, ...] = LAYERS, operations: tuple[str, ...] = OPERATIONS,
                    schema: str | None = None, config: RetentionConfig | None = None,
                    trino: TrinoClient | None = None) -> MaintenanceReport:
    config = config or load_retention()
    unknown = set(operations) - set(OPERATIONS)
    if unknown:
        raise ValueError(f"unknown maintenance operations: {sorted(unknown)}")
    catalogs = [catalog_of(layer) for layer in layers]
    # floor lowered to 1h: retention thresholds below Trino's 7d default are a deliberate policy choice
    trino = trino or TrinoClient(session_properties=iceberg.retention_session(catalogs, "1h"), source="lakehouse-maintenance")
    report = MaintenanceReport()
    for layer in layers:
        policy = config.layers[layer]
        for table in iceberg.list_tables(trino, catalog_of(layer), config.exclude_schemas, schema):
            for op in operations:
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
    log.info("maintenance finished: %d ok, %d failed", len(report.done), len(report.failed))
    return report
