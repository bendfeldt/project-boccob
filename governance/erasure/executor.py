"""Right to erasure - execution, physical purge, verification and audit record.

Sequence (one queued request; see queue.py for why the key is staged in a table)
1. the subject key is staged in the request queue (pyiceberg); SQL references it only through
   the opaque request id - no key, CPR number or pseudonym ever appears in Trino query text
2. collect the native raw files that contain the subject (from the raw index)
3. drop stale dbt work tables (``__dbt_tmp`` / ``__stage``) that may hold copies of the subject
4. run the metadata derived plan: raw index rows, facts, clean/consolidated rows, dimensions+keystore
5. remove the native raw files (all object versions) and leave a tombstone without personal data
6. purge Iceberg history of every touched table (governance.iceberg.purge_history)
7. verify in SQL, then remove the request from the queue and purge the queue's history
8. remove every noncurrent object version and delete marker in the lakehouse buckets: on a
   versioned bucket the files deleted in steps 3-7 are otherwise still readable by version id
9. optionally scan every object version of the lakehouse buckets for the subject; write the audit row

The job must run exclusively (no concurrent pipeline writes): steps 6-8 use a zero retention window.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from governance import iceberg
from governance.classification import manifest as cm
from governance.config import catalog_of, manifest_path
from governance.erasure import queue as rq
from governance.erasure.plan import (
    KEYSTORE_SCHEMA,
    ErasurePlan,
    build_plan,
    subject_query,
    validate_request_id,
    validate_subject_key,
)
from governance.erasure.storage_scan import scan_storage
from ingestion.framework.pseudonym import pseudonymise
from ingestion.framework.spec import list_sources
from lakehouse.objectstore import delete_permanently, purge_noncurrent
from lakehouse.settings import LAYERS, S3Settings, layer_bucket, pseudonym_pepper
from lakehouse.trino_client import TrinoClient, fqn

log = logging.getLogger(__name__)
AUDIT_SCHEMA = "audit"
AUDIT_TABLE = "erasure_log"
WORK_TABLE_SUFFIXES = ("__dbt_tmp", "__stage", "__dbt_backup")  # dbt / dimension materialization
TOMBSTONE_SUFFIX = ".erased.json"


class ErasureVerificationError(RuntimeError):
    pass


@dataclass
class ErasureReport:
    erasure_id: str
    subject_digest: str
    dry_run: bool
    status: str = "planned"
    rows: dict[str, int] = field(default_factory=dict)
    raw_files: list[str] = field(default_factory=list)
    purged_tables: list[str] = field(default_factory=list)
    remaining: dict[str, int] = field(default_factory=dict)
    storage_hits: list[str] = field(default_factory=list)
    dropped_work_tables: list[str] = field(default_factory=list)
    purged_object_versions: int = 0
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def audit_digest(subject_key: str, pepper: str) -> str:
    """Keyed digest stored in the audit log: lets the controller prove that a given identifier
    was erased (by recomputing it) without the log itself identifying anyone."""
    return hmac.new(pepper.encode(), f"erasure-audit|{subject_key}".encode(), hashlib.sha256).hexdigest()


def _table_exists(trino: TrinoClient, name: str) -> bool:
    return trino.table_exists(*name.split("."))


def _ensure_audit_table(trino: TrinoClient) -> str:
    catalog = catalog_of("curated")
    trino.execute(f'create schema if not exists "{catalog}"."{AUDIT_SCHEMA}"')
    table = fqn(catalog, AUDIT_SCHEMA, AUDIT_TABLE)
    trino.execute(
        f"""create table if not exists {table} (
            erasure_id varchar, subject_digest varchar, identifier_type varchar, requested_by varchar,
            reason varchar, status varchar, started_at timestamp(6) with time zone,
            completed_at timestamp(6) with time zone, executed_by varchar, details varchar)
            comment 'Audit log of right-to-erasure executions (contains no personal identifiers)'"""
    )
    return table


def _baseline_tables(trino: TrinoClient) -> list[str]:
    catalog = catalog_of("curated")
    rows = trino.execute(
        f'select table_name from "{catalog}".information_schema.tables where table_schema = ?', [KEYSTORE_SCHEMA]
    )
    return [f"{catalog}.{KEYSTORE_SCHEMA}.{r[0]}" for r in rows if r[0].endswith("__baseline")]


def lake_buckets(s3: S3Settings) -> list[str]:
    return [s3.raw_bucket if layer == "raw" else layer_bucket(layer) for layer in LAYERS]


def _session(trino: TrinoClient | None) -> TrinoClient:
    catalogs = [catalog_of(layer) for layer in LAYERS]
    return trino or TrinoClient(
        session_properties=iceberg.retention_session(catalogs, "0s"), source="lakehouse-erasure"
    )


def drop_work_tables(trino: TrinoClient) -> list[str]:
    """Drop leftovers of failed dbt runs (they are copies of model data, invisible to the plan)."""
    dropped = []
    for layer in ("clean", "consolidated", "curated"):
        catalog = catalog_of(layer)
        rows = trino.execute(
            f"select table_schema, table_name from {fqn(catalog, 'information_schema', 'tables')} "
            "where table_schema <> 'information_schema'"
        )
        for schema, table in rows:
            if table.endswith(WORK_TABLE_SUFFIXES):
                trino.execute(f"drop table if exists {fqn(catalog, schema, table)}")
                dropped.append(f"{catalog}.{schema}.{table}")
    return dropped


def submit_request(
    *,
    identifier: str | None = None,
    identifier_type: str = "cpr",
    subject_key: str | None = None,
    requested_by: str,
    reason: str,
) -> str:
    """Queue an erasure for the nightly job. Only the pseudonymised key is stored."""
    if bool(identifier) == bool(subject_key):
        raise ValueError("give exactly one of identifier / subject_key")
    key = validate_subject_key(subject_key or pseudonymise(identifier, pseudonym_pepper()))
    return rq.enqueue(
        key,
        identifier_type=identifier_type if identifier else "subject_key",
        requested_by=requested_by,
        reason=reason,
    )


def _dequeue(trino: TrinoClient, request_id: str) -> None:
    trino.execute(f"delete from {rq.queue_table_sql()} where request_id = '{validate_request_id(request_id)}'")
    iceberg.purge_history(trino, iceberg.TableRef(catalog_of("raw"), rq.QUEUE_NAMESPACE, rq.QUEUE_TABLE))


def execute_request(
    request_id: str,
    *,
    subject_key: str,
    identifier_type: str,
    requested_by: str,
    reason: str,
    identifier: str | None = None,
    dry_run: bool = False,
    verify_storage: bool = False,
    trino: TrinoClient | None = None,
    s3: S3Settings | None = None,
    purge_s3: S3Settings | None = None,
) -> ErasureReport:
    """Erase the subject of one queued request. ``subject_key`` / ``identifier`` stay in Python:
    they are used for the audit digest and the storage scan, never in SQL."""
    key = validate_subject_key(subject_key)
    pepper = pseudonym_pepper()
    started = datetime.now(UTC)
    report = ErasureReport(erasure_id=str(uuid.uuid4()), subject_digest=audit_digest(key, pepper), dry_run=dry_run)
    trino = _session(trino)
    s3 = s3 or S3Settings.from_env()
    purge_s3 = purge_s3 or S3Settings.for_purge()
    fs, purge_fs = s3.filesystem(), purge_s3.filesystem()
    tables = cm.tables_from_manifest(cm.load_manifest(manifest_path()))
    plan: ErasurePlan = build_plan(request_id, tables, list_sources())
    existing = {name for name in plan.tables if _table_exists(trino, name)}
    steps = [s for s in plan.steps if s.table in existing]

    for step in steps:
        report.rows[f"{step.table} [{step.action}]"] = int(trino.scalar(step.count_sql) or 0)
    files: set[str] = set()
    for table, sql in plan.raw_file_queries:
        if table in existing:
            files.update(r[0] for r in trino.execute(sql))
    report.raw_files = sorted(files)
    baselines = _baseline_tables(trino)
    if dry_run:
        return report

    audit_table = _ensure_audit_table(trino)
    try:
        report.dropped_work_tables = drop_work_tables(trino)
        for step in steps:
            for statement in step.statements:
                trino.execute(statement)
        for name in baselines:
            trino.execute(f"delete from {fqn(*name.split('.'))} where business_key in ({subject_query(request_id)})")

        for path in report.raw_files:
            delete_permanently(purge_fs, s3.raw_bucket, path)
            tombstone = {
                "erased_at": datetime.now(UTC).isoformat(),
                "erasure_id": report.erasure_id,
                "reason": "GDPR art. 17 erasure; records of other data subjects remain in the raw index",
            }
            fs.pipe_file(f"{s3.raw_bucket}/{path}{TOMBSTONE_SUFFIX}", json.dumps(tombstone).encode())

        for name in sorted(existing | set(baselines)):
            iceberg.purge_history(trino, iceberg.TableRef(*name.split(".")))
            report.purged_tables.append(name)

        for step in steps:  # needs the queued key, so before the request is removed
            left = int(trino.scalar(step.verify_sql) or 0)
            if left:
                report.remaining[f"{step.table} [{step.action}]"] = left
        if report.remaining:
            raise ErasureVerificationError(f"subject still present in tables: {report.remaining}")

        _dequeue(trino, request_id)
        for bucket in lake_buckets(s3):
            report.purged_object_versions += purge_noncurrent(purge_fs, bucket)
        if verify_storage:
            needles = [key] + ([identifier] if identifier else [])
            report.storage_hits = scan_storage(purge_fs, lake_buckets(s3), needles)
        if report.storage_hits:
            raise ErasureVerificationError(f"subject still present in objects: {report.storage_hits[:5]}")
        report.status = "completed"
    except Exception as exc:
        report.status = "failed"
        report.error = str(exc)[:1000]
        raise
    finally:
        details = {k: v for k, v in report.as_dict().items() if k not in ("erasure_id", "subject_digest", "status")}
        trino.execute(
            f"insert into {audit_table} values (?, ?, ?, ?, ?, ?, ?, ?, current_user, ?)",
            [
                report.erasure_id,
                report.subject_digest,
                identifier_type,
                requested_by,
                f"{reason} (request {request_id})",
                report.status,
                started,
                datetime.now(UTC),
                json.dumps(details, default=str),
            ],
        )
        log.info("erasure %s %s: %s", report.erasure_id, report.status, json.dumps(details, default=str))
    return report


def erase_subject(
    *,
    identifier: str | None = None,
    identifier_type: str = "cpr",
    subject_key: str | None = None,
    requested_by: str,
    reason: str,
    dry_run: bool = False,
    verify_storage: bool = False,
    trino: TrinoClient | None = None,
    s3: S3Settings | None = None,
) -> ErasureReport:
    """Immediate erasure (CLI): stage the request, execute it. A dry run removes its request again."""
    if bool(identifier) == bool(subject_key):
        raise ValueError("give exactly one of identifier / subject_key")
    key = validate_subject_key(subject_key or pseudonymise(identifier, pseudonym_pepper()))
    id_type = identifier_type if identifier else "subject_key"
    request_id = rq.enqueue(key, identifier_type=id_type, requested_by=requested_by, reason=reason)
    trino = _session(trino)
    try:
        return execute_request(
            request_id,
            subject_key=key,
            identifier=identifier,
            identifier_type=id_type,
            requested_by=requested_by,
            reason=reason,
            dry_run=dry_run,
            verify_storage=verify_storage,
            trino=trino,
            s3=s3,
        )
    finally:
        if dry_run:
            _dequeue(trino, request_id)


def process_pending_requests(
    *, verify_storage: bool = False, trino: TrinoClient | None = None, s3: S3Settings | None = None
) -> list[ErasureReport]:
    """Execute every queued request (nightly Dagster job). A failed request stays queued."""
    rq.ensure_queue()
    trino = _session(trino)
    pending = trino.execute(
        f"select request_id, subject_key, identifier_type, requested_by, reason from {rq.queue_table_sql()} "
        "order by requested_at"
    )
    return [
        execute_request(
            request_id,
            subject_key=key,
            identifier_type=id_type,
            requested_by=requested_by,
            reason=reason,
            verify_storage=verify_storage,
            trino=trino,
            s3=s3,
        )
        for request_id, key, id_type, requested_by, reason in pending
    ]
