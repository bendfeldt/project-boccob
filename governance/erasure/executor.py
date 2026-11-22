"""Right to erasure - execution, physical purge, verification and audit record.

The nightly job executes every queued request in one batch (:func:`process_pending_requests`):

1. drop stale dbt work tables (``__dbt_tmp`` / ``__stage`` / ``__dbt_backup``): copies of model
   data that the plan cannot see
2. per request: the metadata derived plan (raw index rows, facts, clean/consolidated rows,
   dimensions + keystore, keystore baselines); the subject's native raw files are removed with all
   object versions and replaced by a tombstone without personal data. SQL references the subject
   only through the opaque request id - no key, CPR number or pseudonym is ever in Trino query text
3. once per touched table, for the whole batch: purge the Iceberg history
   (governance.iceberg.purge_history), then every noncurrent object version under the table's
   location (on a versioned bucket the files deleted above are still readable by version id).
   N requests therefore cost one rewrite per table, not N. The rest of the lake keeps its deletion
   safety window (NONCURRENT_VERSION_GRACE_DAYS / bucket lifecycle)
4. per request: verify in SQL, write the audit row (exactly one), then remove the request from
   the queue
5. purge the queue's own history and noncurrent versions (they held the keys)

A request that fails at any step stays in the queue with ``status = 'failed'`` (the key is kept,
so the next run retries it without the DPO resubmitting the identifier); the other requests are
still processed and the batch fails at the end (:class:`ErasureBatchError`).

The job must run exclusively (no concurrent pipeline writes): step 3 uses a zero retention window.
Outside dev only the Dagster job (writer pool) executes erasures; the CLI only queues them.
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
    build_plan,
    subject_query,
    validate_request_id,
    validate_subject_key,
)
from ingestion.framework.pseudonym import pseudonymise
from ingestion.framework.spec import list_sources
from lakehouse.objectstore import delete_permanently, purge_noncurrent
from lakehouse.settings import LAYERS, S3Settings, pseudonym_pepper
from lakehouse.trino_client import TrinoClient, fqn

log = logging.getLogger(__name__)
AUDIT_SCHEMA = "audit"
AUDIT_TABLE = "erasure_log"
WORK_TABLE_SUFFIXES = ("__dbt_tmp", "__stage", "__dbt_backup")  # dbt / dimension materialization
TOMBSTONE_SUFFIX = ".erased.json"
DRY_RUN_REQUEST = "00000000-0000-0000-0000-000000000000"  # placeholder id: a dry run queues nothing


class ErasureVerificationError(RuntimeError):
    pass


class ErasureAuditError(RuntimeError):
    """The audit row was not written exactly once: the erasure is not proven, so it is not done."""


class ErasureBatchError(RuntimeError):
    """At least one queued request failed; every other request was still processed."""

    def __init__(self, reports: list[ErasureReport]):
        self.reports = reports
        failed = [r for r in reports if r.status != "completed"]
        super().__init__(
            f"{len(failed)} of {len(reports)} erasure request(s) failed and stay queued: "
            + "; ".join(f"{r.request_id}: {r.error}" for r in failed)
        )


@dataclass
class ErasureReport:
    erasure_id: str
    subject_digest: str
    dry_run: bool
    request_id: str | None = None
    scope: str = "full"
    status: str = "planned"
    rows: dict[str, int] = field(default_factory=dict)
    raw_files: list[str] = field(default_factory=list)
    purged_tables: list[str] = field(default_factory=list)
    remaining: dict[str, int] = field(default_factory=dict)
    dropped_work_tables: list[str] = field(default_factory=list)
    purged_object_versions: int = 0  # for the whole batch
    planned: list[str] = field(default_factory=list)  # dry run: "<table> [<action>]"
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


def table_location(trino: TrinoClient, name: str) -> str | None:
    """``bucket/prefix/`` of an Iceberg table, from its metadata log."""
    catalog, schema, table = name.split(".")
    metadata_log = fqn(catalog, schema, table)[:-1] + '$metadata_log_entries"'  # "c"."s"."t$metadata_log_entries"
    path = trino.scalar(f"select file from {metadata_log} limit 1")
    if not path or "/metadata/" not in path:
        return None
    return path.split("://", 1)[-1].rsplit("/metadata/", 1)[0] + "/"


def _session(trino: TrinoClient | None) -> TrinoClient:
    catalogs = [catalog_of(layer) for layer in LAYERS]
    return trino or TrinoClient(
        session_properties=iceberg.retention_session(catalogs, "0s"), source="lakehouse-erasure"
    )


def drop_work_tables(trino: TrinoClient, locations: list[str] | None = None) -> list[str]:
    """Drop leftovers of failed dbt runs (they are copies of model data, invisible to the plan).
    Their storage locations are appended to ``locations`` (purged by the erasure afterwards)."""
    dropped = []
    for layer in ("clean", "consolidated", "curated"):
        catalog = catalog_of(layer)
        rows = trino.execute(
            f"select table_schema, table_name from {fqn(catalog, 'information_schema', 'tables')} "
            "where table_schema <> 'information_schema'"
        )
        for schema, table in rows:
            if table.endswith(WORK_TABLE_SUFFIXES):
                if locations is not None:
                    location = table_location(trino, f"{catalog}.{schema}.{table}")
                    if location:
                        locations.append(location)
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


def _mark_failed(trino: TrinoClient, request_id: str) -> None:
    """Keep the request (and its key) for the next run; only the status changes."""
    trino.execute(
        f"update {rq.queue_table_sql()} set status = 'failed', attempts = coalesce(attempts, 0) + 1, "
        f"updated_at = current_timestamp where request_id = '{validate_request_id(request_id)}'"
    )


def _write_audit(trino: TrinoClient, audit_table: str, report: ErasureReport, request: dict, started) -> None:
    """Insert the audit row and insist on exactly one inserted row. The keyed digest is a query
    parameter: the platform audit sink redacts 64-hex literals in archived query text."""
    details = {k: v for k, v in report.as_dict().items() if k not in ("erasure_id", "subject_digest", "status")}
    result = trino.execute(
        f"insert into {audit_table} values (?, ?, ?, ?, ?, ?, ?, ?, current_user, ?)",
        [
            report.erasure_id,
            report.subject_digest,
            request["identifier_type"],
            request["requested_by"],
            f"{request['reason']} (request {report.request_id})",
            report.status,
            started,
            datetime.now(UTC),
            json.dumps(details, default=str),
        ],
    )
    inserted = result[0][0] if result and result[0] else 0
    if inserted != 1:
        raise ErasureAuditError(f"audit log insert for erasure {report.erasure_id} wrote {inserted} rows, expected 1")
    log.info("erasure %s %s: %s", report.erasure_id, report.status, json.dumps(details, default=str))


def _purge_queue(trino: TrinoClient, purge_fs) -> int:
    """The queue held the keys of finished requests: purge its history and noncurrent versions."""
    iceberg.purge_history(trino, iceberg.TableRef(catalog_of("raw"), rq.QUEUE_NAMESPACE, rq.QUEUE_TABLE))
    bucket, _, prefix = rq.queue_location().partition("/")
    return purge_noncurrent(purge_fs, bucket, prefix)


def _error(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"[:1000]


def execute_batch(
    requests: list[dict],
    *,
    trino: TrinoClient | None = None,
    s3: S3Settings | None = None,
    purge_s3: S3Settings | None = None,
) -> list[ErasureReport]:
    """Erase the subjects of queued requests (dicts with the queue columns), see the module doc.
    Subject keys stay in Python (audit digest), never in SQL."""
    if not requests:
        return []
    pepper = pseudonym_pepper()
    trino = _session(trino)
    s3 = s3 or S3Settings.from_env()
    purge_s3 = purge_s3 or S3Settings.for_purge()
    fs, purge_fs = s3.filesystem(), purge_s3.filesystem()
    tables = cm.tables_from_manifest(cm.load_manifest(manifest_path()))
    sources = list_sources()
    audit_table = _ensure_audit_table(trino)
    baselines = _baseline_tables(trino)
    exists: dict[str, bool] = {}
    locations: list[str] = []
    dropped = drop_work_tables(trino, locations)
    touched: set[str] = set()
    work: list[tuple[ErasureReport, dict, list, datetime, Exception | None]] = []

    # 1. the deletes and updates of every request
    for request in requests:
        started = datetime.now(UTC)
        report = ErasureReport(
            erasure_id=str(uuid.uuid4()),
            subject_digest="-",
            dry_run=False,
            request_id=request["request_id"],
            scope=request["scope"],
            dropped_work_tables=dropped,
        )
        steps: list = []
        try:
            request_id = validate_request_id(request["request_id"])
            report.subject_digest = audit_digest(validate_subject_key(request["subject_key"]), pepper)
            plan = build_plan(request_id, tables, sources, scope=request["scope"])
            for name in plan.tables:
                if name not in exists:
                    exists[name] = _table_exists(trino, name)
            steps = [s for s in plan.steps if exists[s.table]]
            for step in steps:
                report.rows[f"{step.table} [{step.action}]"] = int(trino.scalar(step.count_sql) or 0)
            files: set[str] = set()
            for table, sql in plan.raw_file_queries:
                if exists.get(table):
                    files.update(r[0] for r in trino.execute(sql))
            report.raw_files = sorted(files)
            for step in steps:
                for statement in step.statements:
                    trino.execute(statement)
            for name in baselines:
                trino.execute(
                    f"delete from {fqn(*name.split('.'))} where business_key in ({subject_query(request_id)})"
                )
            for path in report.raw_files:
                delete_permanently(purge_fs, s3.raw_bucket, path)
                tombstone = {
                    "erased_at": datetime.now(UTC).isoformat(),
                    "erasure_id": report.erasure_id,
                    "reason": "GDPR art. 17 erasure; records of other data subjects remain in the raw index",
                }
                fs.pipe_file(f"{s3.raw_bucket}/{path}{TOMBSTONE_SUFFIX}", json.dumps(tombstone).encode())
            touched.update(s.table for s in steps)
            touched.update(baselines)
            work.append((report, request, steps, started, None))
        except Exception as exc:
            log.exception("erasure request %s failed before the purge", request.get("request_id"))
            work.append((report, request, steps, started, exc))

    # 2. once per touched table, for the whole batch
    purged_tables: list[str] = []
    purge_failure: Exception | None = None
    versions = 0
    try:
        for name in sorted(touched):
            location = table_location(trino, name)
            if location:
                locations.append(location)
            iceberg.purge_history(trino, iceberg.TableRef(*name.split(".")))
            purged_tables.append(name)
        for location in locations:
            bucket, _, prefix = location.partition("/")
            versions += purge_noncurrent(purge_fs, bucket, prefix)
    except Exception as exc:
        log.exception("erasure batch: purge failed; every request of the batch stays queued")
        purge_failure = exc

    # 3. per request: verify, audit, dequeue - or keep it queued as failed
    reports = []
    for report, request, steps, started, failure in work:
        report.purged_tables = purged_tables
        report.purged_object_versions = versions
        failure = failure or purge_failure
        if failure is None:
            try:
                for step in steps:  # needs the queued key, so before the request is removed
                    left = int(trino.scalar(step.verify_sql) or 0)
                    if left:
                        report.remaining[f"{step.table} [{step.action}]"] = left
                if report.remaining:
                    raise ErasureVerificationError(f"subject still present in tables: {report.remaining}")
            except Exception as exc:
                failure = exc
        report.status = "failed" if failure else "completed"
        report.error = _error(failure) if failure else None
        try:
            _write_audit(trino, audit_table, report, request, started)
            if failure is None:
                trino.execute(
                    f"delete from {rq.queue_table_sql()} where request_id = '{validate_request_id(report.request_id)}'"
                )
        except Exception as exc:
            if failure is not None:
                log.error("erasure %s: audit row could not be written either: %s", report.erasure_id, exc)
            failure = failure or exc
            report.status, report.error = "failed", _error(failure)
        if failure is not None:
            try:
                _mark_failed(trino, report.request_id)
            except Exception as exc:  # still queued (status unchanged): it is retried anyway
                log.error("erasure request %s: could not mark it failed: %s", report.request_id, exc)
            log.error("erasure request %s failed (kept in the queue): %s", report.request_id, report.error)
        reports.append(report)
    return reports


def preview(*, scope: str = "full") -> ErasureReport:
    """Dry run: the tables and actions an erasure would apply, from the same plan (no identifier,
    no Trino query, nothing written). The row counts are reported by the executed job."""
    tables = cm.tables_from_manifest(cm.load_manifest(manifest_path()))
    plan = build_plan(DRY_RUN_REQUEST, tables, list_sources(), scope=scope)
    report = ErasureReport(erasure_id="dry-run", subject_digest="-", dry_run=True, scope=scope)
    report.planned = [f"{s.table} [{s.action}]" for s in plan.steps]
    report.planned += [f"native raw files located through {t} [delete all versions]" for t, _ in plan.raw_file_queries]
    return report


def _requests(trino: TrinoClient, request_id: str | None = None) -> list[dict]:
    where = f" where request_id = '{validate_request_id(request_id)}'" if request_id else ""
    rows = trino.execute(
        "select request_id, subject_key, identifier_type, requested_by, reason, scope "
        f"from {rq.queue_table_sql()}{where} order by requested_at"
    )
    names = ("request_id", "subject_key", "identifier_type", "requested_by", "reason", "scope")
    return [dict(zip(names, r, strict=True)) for r in rows]


def _finish(trino: TrinoClient, reports: list[ErasureReport], purge_s3: S3Settings | None) -> list[ErasureReport]:
    """Purge the queue's history (every run: retried if it failed before), then fail the run when
    any request failed."""
    failed = any(r.status != "completed" for r in reports)
    if reports:
        try:
            _purge_queue(trino, (purge_s3 or S3Settings.for_purge()).filesystem())
        except Exception:
            log.exception("erasure: purge of the queue history failed (retried by the next run)")
            if not failed:
                raise
    if failed:
        raise ErasureBatchError(reports)
    return reports


def erase_subject(
    *,
    identifier: str | None = None,
    identifier_type: str = "cpr",
    subject_key: str | None = None,
    requested_by: str,
    reason: str,
    trino: TrinoClient | None = None,
    s3: S3Settings | None = None,
) -> ErasureReport:
    """Immediate erasure (CLI, dev only): queue the request, execute it."""
    if bool(identifier) == bool(subject_key):
        raise ValueError("give exactly one of identifier / subject_key")
    key = validate_subject_key(subject_key or pseudonymise(identifier, pseudonym_pepper()))
    id_type = identifier_type if identifier else "subject_key"
    request_id = rq.enqueue(key, identifier_type=id_type, requested_by=requested_by, reason=reason)
    trino = _session(trino)
    reports = execute_batch(_requests(trino, request_id), trino=trino, s3=s3)
    return _finish(trino, reports, None)[0]


def process_pending_requests(
    *,
    trino: TrinoClient | None = None,
    s3: S3Settings | None = None,
    purge_s3: S3Settings | None = None,
) -> list[ErasureReport]:
    """Execute every queued request in one batch (nightly Dagster job), oldest first, including
    requests that failed before. Raises :class:`ErasureBatchError` at the end when any failed."""
    rq.ensure_queue()
    trino = _session(trino)
    reports = execute_batch(_requests(trino), trino=trino, s3=s3, purge_s3=purge_s3)
    return _finish(trino, reports, purge_s3)
