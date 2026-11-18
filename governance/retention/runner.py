"""Time based retention: raw landing files, raw index rows and tables with a retention rule."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from governance.classification import manifest as cm
from governance.config import RetentionConfig, catalog_of, load_retention, manifest_path
from ingestion.framework.manifest import MANIFEST_TABLE, load_date_of
from ingestion.framework.spec import SourceSpec, list_sources
from lakehouse.objectstore import delete_permanently
from lakehouse.settings import S3Settings
from lakehouse.trino_client import TrinoClient, fqn, ident

log = logging.getLogger(__name__)
_LOAD_DATE = re.compile(r"load_date=(\d{4}-\d{2}-\d{2})/([0-9.]+)/")


@dataclass(frozen=True)
class DeleteStatement:
    table: str
    sql: str
    reason: str


@dataclass
class RetentionPlan:
    statements: list[DeleteStatement] = field(default_factory=list)
    landing_cutoff: date | None = None
    uncommitted_cutoff: date | None = None
    skipped: list[str] = field(default_factory=list)  # tables without a time based rule
    deregistrations: list[Deregistration] = field(default_factory=list)


@dataclass(frozen=True)
class Deregistration:
    """Subjects whose latest state in ``table`` is deregistered since before ``cutoff``."""

    table: str
    select_sql: str  # yields subject keys due for erasure; contains no key literal
    reason: str


def deregistration_select(t: cm.TableClass, rule: dict, cutoff: date, queue_sql: str) -> str:
    subject, flag, since = (ident(t.gdpr["subject_column"]), ident(rule["flag_column"]), ident(rule["since_column"]))
    return (
        f"select s.subject from (select {subject} as subject, max_by({flag}, {since}) as flag, "
        f"max({since}) as since from {fqn(t.catalog, t.schema, t.table)} group by 1) s "
        f"where s.flag and s.since < timestamp '{cutoff.isoformat()} 00:00:00 UTC' "
        f"and s.subject not in (select subject_key from {queue_sql})"
    )


def plan_retention(
    tables: list[cm.TableClass], sources: list[SourceSpec], config: RetentionConfig, today: date
) -> RetentionPlan:
    """Pure planning step (unit tested): which rows/files have outlived their retention."""
    plan = RetentionPlan()
    raw = config.layers["raw"]
    if raw.landing_days is not None:
        plan.landing_cutoff = today - timedelta(days=raw.landing_days)
    if raw.uncommitted_landing_days is not None:
        plan.uncommitted_cutoff = today - timedelta(days=raw.uncommitted_landing_days)
    if raw.index_days is not None:
        cutoff = today - timedelta(days=raw.index_days)
        for source in sources:
            for entity in source.entities:
                table = fqn(catalog_of("raw"), source.name, entity.name)
                where = f"_loaded_at < timestamp '{cutoff.isoformat()} 00:00:00 UTC'"
                if entity.snapshot:
                    # reference snapshots: the newest delivery stays, whatever its age
                    where += f" and _load_id <> (select max_by(_load_id, cast(_load_id as double)) from {table})"
                plan.statements.append(
                    DeleteStatement(
                        table=f"{catalog_of('raw')}.{source.name}.{entity.name}",
                        sql=f"delete from {table} where {where}",
                        reason=f"raw index retention {raw.index_days}d"
                        + (" (latest snapshot kept)" if entity.snapshot else ""),
                    )
                )
    from governance.erasure.queue import queue_table_sql

    for t in tables:
        if t.resource_type != "model":
            continue
        rule = t.gdpr.get("deregistration")
        if rule:
            cutoff = today - timedelta(days=int(rule["grace_days"]))
            plan.deregistrations.append(
                Deregistration(
                    table=t.key,
                    select_sql=deregistration_select(t, rule, cutoff, queue_table_sql()),
                    reason=f"deregistered for more than {rule['grace_days']} days ({t.key})",
                )
            )
            continue
        days, column = t.gdpr.get("retention_days"), t.gdpr.get("retention_column")
        if not days or not column:
            if any(c.pii in ("indirect", "direct", "sensitive") for c in t.columns):
                plan.skipped.append(t.key)
            continue
        cutoff = today - timedelta(days=int(days))
        plan.statements.append(
            DeleteStatement(
                table=t.key,
                sql=(
                    f"delete from {fqn(t.catalog, t.schema, t.table)} "
                    f"where cast({ident(column)} as date) < date '{cutoff.isoformat()}'"
                ),
                reason=f"retention {days}d on {column}",
            )
        )
    return plan


def expired_landing_objects(
    keys: list[str], committed_load_ids: set[str], landing_cutoff: date | None, uncommitted_cutoff: date | None
) -> list[str]:
    """Select object keys (relative to the bucket) whose retention has expired."""
    expired = []
    for key in keys:
        m = _LOAD_DATE.search(key)
        if not m:
            continue
        load_date, load_id = date.fromisoformat(m.group(1)), m.group(2)
        too_old = bool(landing_cutoff and load_date < landing_cutoff)
        aborted = bool(uncommitted_cutoff and load_id not in committed_load_ids and load_date < uncommitted_cutoff)
        if too_old or aborted:
            expired.append(key)
    return expired


def queue_deregistered(trino: TrinoClient, plan: RetentionPlan, *, dry_run: bool = False) -> list[dict[str, Any]]:
    """Insert the keys of subjects due for erasure into the erasure queue (INSERT ... SELECT:
    the keys never appear in SQL text). Returns the count per table."""
    from governance.erasure.queue import ensure_queue, queue_table_sql

    if not plan.deregistrations:
        return []
    ensure_queue()
    counts = []
    for d in plan.deregistrations:
        if dry_run:
            count = trino.scalar(f"select count(*) from ({d.select_sql})")
        else:
            result = trino.execute(
                f"insert into {queue_table_sql()} "
                "select cast(uuid() as varchar), subject, 'subject_key', 'retention-policy', "
                f"'{d.reason}', current_timestamp from ({d.select_sql})"
            )
            count = result[0][0] if result and result[0] else 0
        counts.append({"table": d.table, "queued_for_erasure": count})
    return counts


def run_retention(
    *, dry_run: bool = False, today: date | None = None, trino: TrinoClient | None = None, s3: S3Settings | None = None
) -> dict[str, Any]:
    today = today or datetime.now(UTC).date()
    config = load_retention()
    sources = list_sources()
    tables = cm.tables_from_manifest(cm.load_manifest(manifest_path()))
    plan = plan_retention(tables, sources, config, today)
    trino = trino or TrinoClient(source="lakehouse-retention")
    s3 = s3 or S3Settings.from_env()
    fs = s3.filesystem()
    purge_fs = S3Settings.for_purge().filesystem()  # removes every version, not only the current one
    report: dict[str, Any] = {
        "dry_run": dry_run,
        "date": today.isoformat(),
        "tables": [],
        "landing_objects": 0,
        "no_time_rule": plan.skipped,
        "deregistered_subjects": [],
    }

    for stmt in plan.statements:
        catalog, schema, table = stmt.table.split(".")
        if not trino.table_exists(catalog, schema, table):
            continue
        if dry_run:
            where = stmt.sql.split(" where ", 1)[1]
            rows = trino.scalar(f"select count(*) from {fqn(catalog, schema, table)} where {where}")
        else:
            result = trino.execute(stmt.sql)
            rows = result[0][0] if result and result[0] else 0
        report["tables"].append({"table": stmt.table, "rows": rows, "reason": stmt.reason})

    # Master data has no time rule while the person is registered. Deregistered subjects are
    # queued for the erasure job (all layers, keystore anonymised) once the grace period is over.
    report["deregistered_subjects"] = queue_deregistered(trino, plan, dry_run=dry_run)

    for source in sources:
        prefix = f"{s3.raw_bucket}/{source.name}/"
        keys = [p[len(s3.raw_bucket) + 1 :] for p in fs.find(prefix)]
        manifests = [k for k in keys if k.startswith(f"{source.name}/{MANIFEST_TABLE}/")]
        committed = {k.rsplit("/", 1)[1].rsplit(".", 2)[0] for k in manifests}
        expired = expired_landing_objects(keys, committed, plan.landing_cutoff, plan.uncommitted_cutoff)
        if plan.landing_cutoff:
            expired += [
                k
                for k in manifests
                if date.fromisoformat(load_date_of(k.rsplit("/", 1)[1].rsplit(".", 2)[0])) < plan.landing_cutoff
            ]
        report["landing_objects"] += len(expired)
        if not dry_run:
            for key in expired:
                delete_permanently(purge_fs, s3.raw_bucket, key)
    log.info("retention %s", report)
    return report
