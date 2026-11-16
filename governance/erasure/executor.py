"""Right to erasure - execution, physical purge, verification and audit record.

Sequence
1. resolve the data subject to its pseudonymised key (HMAC with the runtime pepper)
2. collect the native raw files that contain the subject (from the raw index)
3. run the metadata derived plan: raw index rows, facts, clean/consolidated rows, dimensions+keystore
4. remove the native raw files (all object versions) and leave a tombstone without personal data
5. purge Iceberg history of every touched table: optimize (rewrites files that carry deletes),
   expire_snapshots(0s), remove_orphan_files(0s) - afterwards no data file, delete file or manifest
   of an older snapshot exists any more
6. verify (SQL, optionally a deep scan of the object store) and write the audit record

The job must run exclusively (no concurrent pipeline writes): step 5 uses a zero retention window.
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
from governance.erasure.plan import KEYSTORE_SCHEMA, ErasurePlan, build_plan, validate_subject_key
from governance.erasure.storage_scan import scan_storage
from ingestion.framework.pseudonym import pseudonymise
from ingestion.framework.spec import list_sources
from lakehouse.objectstore import delete_permanently
from lakehouse.settings import LAYERS, S3Settings, pseudonym_pepper
from lakehouse.trino_client import TrinoClient, fqn

log = logging.getLogger(__name__)
AUDIT_SCHEMA = "audit"
AUDIT_TABLE = "erasure_log"
REQUEST_TABLE = "erasure_requests"
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


def erase_subject(*, identifier: str | None = None, identifier_type: str = "cpr", subject_key: str | None = None,
                  requested_by: str, reason: str, dry_run: bool = False, verify_storage: bool = False,
                  trino: TrinoClient | None = None, s3: S3Settings | None = None) -> ErasureReport:
    if bool(identifier) == bool(subject_key):
        raise ValueError("give exactly one of identifier / subject_key")
    pepper = pseudonym_pepper()
    key = validate_subject_key(subject_key or pseudonymise(identifier, pepper))
    started = datetime.now(UTC)
    report = ErasureReport(erasure_id=str(uuid.uuid4()), subject_digest=audit_digest(key, pepper), dry_run=dry_run)

    catalogs = [catalog_of(layer) for layer in LAYERS]
    trino = trino or TrinoClient(session_properties=iceberg.retention_session(catalogs, "0s"), source="lakehouse-erasure")
    s3 = s3 or S3Settings.from_env()
    fs = s3.filesystem()
    tables = cm.tables_from_manifest(cm.load_manifest(manifest_path()))
    plan: ErasurePlan = build_plan(key, tables, list_sources())
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
        for step in steps:
            for statement in step.statements:
                trino.execute(statement)
        for name in baselines:
            trino.execute(f"delete from {fqn(*name.split('.'))} where business_key = '{key}'")

        for path in report.raw_files:
            delete_permanently(fs, s3.raw_bucket, path)
            tombstone = {"erased_at": datetime.now(UTC).isoformat(), "erasure_id": report.erasure_id,
                         "reason": "GDPR art. 17 erasure; records of other data subjects remain in the raw index"}
            fs.pipe_file(f"{s3.raw_bucket}/{path}{TOMBSTONE_SUFFIX}", json.dumps(tombstone).encode())

        for name in sorted(existing | set(baselines)):
            iceberg.purge_history(trino, iceberg.TableRef(*name.split(".")))
            report.purged_tables.append(name)

        for step in steps:
            left = int(trino.scalar(step.verify_sql) or 0)
            if left:
                report.remaining[f"{step.table} [{step.action}]"] = left
        if verify_storage:
            needles = [key] + ([identifier] if identifier else [])
            report.storage_hits = scan_storage(fs, [f"lakehouse-{layer}" if layer != "raw" else s3.raw_bucket
                                                    for layer in LAYERS], needles)
        if report.remaining or report.storage_hits:
            raise ErasureVerificationError(
                f"subject still present: tables={report.remaining} objects={report.storage_hits[:5]}")
        report.status = "completed"
    except Exception as exc:
        report.status = "failed"
        report.error = str(exc)[:1000]
        raise
    finally:
        details = {k: v for k, v in report.as_dict().items() if k not in ("erasure_id", "subject_digest", "status")}
        trino.execute(
            f"insert into {audit_table} values (?, ?, ?, ?, ?, ?, ?, ?, current_user, ?)",
            [report.erasure_id, report.subject_digest, identifier_type if identifier else "subject_key",
             requested_by, reason, report.status, started, datetime.now(UTC), json.dumps(details, default=str)],
        )
        log.info("erasure %s %s: %s", report.erasure_id, report.status, json.dumps(details, default=str))
    return report


# --- request queue ---------------------------------------------------------------------------
# The orchestrator must never see an identifier in run configuration or logs. The DPO tooling
# therefore *submits* a request (only the pseudonymised key is stored, in a restricted schema)
# and the scheduled erasure job processes whatever is pending and removes the request again.

def _ensure_request_table(trino: TrinoClient) -> str:
    catalog = catalog_of("curated")
    trino.execute(f'create schema if not exists "{catalog}"."{AUDIT_SCHEMA}"')
    table = fqn(catalog, AUDIT_SCHEMA, REQUEST_TABLE)
    trino.execute(
        f"""create table if not exists {table} (
            request_id varchar, subject_key varchar, identifier_type varchar, requested_by varchar,
            reason varchar, requested_at timestamp(6) with time zone)
            comment 'Pending right-to-erasure requests; rows are removed and purged once executed'"""
    )
    return table


def submit_request(*, identifier: str | None = None, identifier_type: str = "cpr", subject_key: str | None = None,
                   requested_by: str, reason: str, trino: TrinoClient | None = None) -> str:
    if bool(identifier) == bool(subject_key):
        raise ValueError("give exactly one of identifier / subject_key")
    key = validate_subject_key(subject_key or pseudonymise(identifier, pseudonym_pepper()))
    trino = trino or TrinoClient(source="lakehouse-erasure")
    table = _ensure_request_table(trino)
    request_id = str(uuid.uuid4())
    trino.execute(f"insert into {table} values (?, ?, ?, ?, ?, current_timestamp)",
                  [request_id, key, identifier_type if identifier else "subject_key", requested_by, reason])
    return request_id


def process_pending_requests(*, verify_storage: bool = False, trino: TrinoClient | None = None,
                             s3: S3Settings | None = None) -> list[ErasureReport]:
    catalogs = [catalog_of(layer) for layer in LAYERS]
    trino = trino or TrinoClient(session_properties=iceberg.retention_session(catalogs, "0s"), source="lakehouse-erasure")
    table = _ensure_request_table(trino)
    pending = trino.execute(f"select request_id, subject_key, requested_by, reason from {table} order by requested_at")
    reports = []
    for request_id, key, requested_by, reason in pending:
        report = erase_subject(subject_key=key, requested_by=requested_by, reason=f"{reason} (request {request_id})",
                               verify_storage=verify_storage, trino=trino, s3=s3)
        trino.execute(f"delete from {table} where request_id = ?", [request_id])
        reports.append(report)
    if pending:
        iceberg.purge_history(trino, iceberg.TableRef(catalog_of("curated"), AUDIT_SCHEMA, REQUEST_TABLE))
    return reports
