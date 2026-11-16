"""Iceberg table maintenance through Trino (optimize, expire_snapshots, remove_orphan_files)."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from lakehouse.trino_client import TrinoClient, fqn, ident

log = logging.getLogger(__name__)
_DURATION = re.compile(r"^\d+(s|m|h|d)$")
_SIZE = re.compile(r"^\d+(kB|MB|GB)$")


@dataclass(frozen=True)
class TableRef:
    catalog: str
    schema: str
    table: str

    @property
    def sql(self) -> str:
        return fqn(self.catalog, self.schema, self.table)

    def __str__(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


def retention_session(catalogs: list[str], minimum: str = "0s") -> dict[str, str]:
    """Session properties that lower Trino's safety floor for snapshot/orphan retention.

    Trino refuses thresholds below ``iceberg.expire-snapshots.min-retention`` (7d by default);
    the session override is permitted only to principals with write access (see the OPA policy).
    """
    if not _DURATION.match(minimum):
        raise ValueError(f"invalid duration {minimum!r}")
    props = {}
    for catalog in catalogs:
        ident(catalog)
        props[f"{catalog}.expire_snapshots_min_retention"] = minimum
        props[f"{catalog}.remove_orphan_files_min_retention"] = minimum
    return props


def list_tables(trino: TrinoClient, catalog: str, exclude_schemas: tuple[str, ...] = ("information_schema", "system"),
                schema: str | None = None) -> list[TableRef]:
    rows = trino.execute(
        f"select table_schema, table_name from {ident(catalog)}.information_schema.tables "
        "where table_type = 'BASE TABLE' order by 1, 2"
    )
    return [
        TableRef(catalog, s, t)
        for s, t in rows
        if s not in exclude_schemas and (schema is None or s == schema)
    ]


def optimize(trino: TrinoClient, table: TableRef, file_size_threshold: str | None = None) -> None:
    """Compact data files; files carrying row-level deletes are rewritten without the deleted rows."""
    if file_size_threshold is None:
        trino.execute(f"alter table {table.sql} execute optimize")
    else:
        if not _SIZE.match(file_size_threshold):
            raise ValueError(f"invalid size {file_size_threshold!r}")
        trino.execute(f"alter table {table.sql} execute optimize(file_size_threshold => '{file_size_threshold}')")


def expire_snapshots(trino: TrinoClient, table: TableRef, retention: str) -> None:
    if not _DURATION.match(retention):
        raise ValueError(f"invalid duration {retention!r}")
    trino.execute(f"alter table {table.sql} execute expire_snapshots(retention_threshold => '{retention}')")


def remove_orphan_files(trino: TrinoClient, table: TableRef, retention: str) -> None:
    if not _DURATION.match(retention):
        raise ValueError(f"invalid duration {retention!r}")
    trino.execute(f"alter table {table.sql} execute remove_orphan_files(retention_threshold => '{retention}')")


def purge_history(trino: TrinoClient, table: TableRef) -> None:
    """Make deleted rows physically unrecoverable: rewrite, drop all old snapshots, drop orphans.

    Requires a connection created with :func:`retention_session` (0s floor). Must not run while
    other writers are active on the table: files of in-flight commits would count as orphans.
    """
    optimize(trino, table)
    expire_snapshots(trino, table, "0s")
    remove_orphan_files(trino, table, "0s")
