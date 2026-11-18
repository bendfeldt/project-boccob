"""Erasure request queue: ``raw.governance.erasure_requests`` (Iceberg, written through pyiceberg).

The queue is the only place where the subject key of a pending erasure is stored, and it is
written without Trino on purpose: Trino query text is archived in the WORM audit trail, so a key
that reaches SQL text could never be erased again. Every erasure statement references the key
through ``(select subject_key from <queue> where request_id = '<uuid>')``.

It lives in the raw catalog because the pipeline principal and the pipeline S3 user already write
there; the schema is reserved (no source may be called ``governance``) and protected by the OPA
policy (only the service group writes, auditors read).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pyarrow as pa

from governance.config import catalog_of
from lakehouse.settings import PolarisSettings, S3Settings
from lakehouse.trino_client import fqn

QUEUE_NAMESPACE = "governance"
QUEUE_TABLE = "erasure_requests"
QUEUE_SCHEMA = pa.schema(
    [
        pa.field("request_id", pa.string(), nullable=False),
        pa.field("subject_key", pa.string(), nullable=False),
        pa.field("identifier_type", pa.string(), nullable=False),
        pa.field("requested_by", pa.string(), nullable=False),
        pa.field("reason", pa.string(), nullable=False),
        pa.field("requested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        # added in release 0.2 (nullable: rows of older queues read as pending / full erasure)
        pa.field("scope", pa.string()),  # full (art. 17 request) | deregistration (retention policy)
        pa.field("status", pa.string()),  # pending | failed (a request leaves the queue only when done)
        pa.field("attempts", pa.int32()),
        pa.field("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)
SCOPES = ("full", "deregistration")


DIGEST_TABLE = "erasure_digests"
DIGEST_SCHEMA = pa.schema(
    [
        pa.field("erasure_id", pa.string(), nullable=False),
        pa.field("subject_digest", pa.string(), nullable=False),
        pa.field("staged_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def digest_table_sql() -> str:
    return fqn(catalog_of("raw"), QUEUE_NAMESPACE, DIGEST_TABLE)


def stage_digest(erasure_id: str, subject_digest: str, catalog=None) -> None:
    """The keyed audit digest reaches curated.audit.erasure_log through INSERT ... SELECT from
    here, so it does not appear in query text either (with the pepper it would confirm a CPR)."""
    catalog = catalog or _catalog()
    catalog.create_namespace_if_not_exists(QUEUE_NAMESPACE)
    table = catalog.create_table_if_not_exists(f"{QUEUE_NAMESPACE}.{DIGEST_TABLE}", schema=DIGEST_SCHEMA)
    row = {"erasure_id": erasure_id, "subject_digest": subject_digest, "staged_at": datetime.now(UTC)}
    table.append(pa.Table.from_pylist([row], schema=DIGEST_SCHEMA))


def queue_table_sql() -> str:
    return fqn(catalog_of("raw"), QUEUE_NAMESPACE, QUEUE_TABLE)


def _catalog(polaris: PolarisSettings | None = None, s3: S3Settings | None = None):
    from pyiceberg.catalog import load_catalog

    polaris = polaris or PolarisSettings.from_env()
    s3 = s3 or S3Settings.from_env()
    return load_catalog(polaris.raw_catalog, **polaris.catalog_properties(polaris.raw_catalog, s3))


def ensure_queue(catalog=None):
    """Create the queue, or add the columns a newer release introduced (schema evolution by name;
    the queue is owned by this code, not by dbt, so it evolves itself instead of via a migration)."""
    catalog = catalog or _catalog()
    catalog.create_namespace_if_not_exists(QUEUE_NAMESPACE)
    table = catalog.create_table_if_not_exists(f"{QUEUE_NAMESPACE}.{QUEUE_TABLE}", schema=QUEUE_SCHEMA)
    existing = {f.name for f in table.schema().fields}
    if not {f.name for f in QUEUE_SCHEMA} <= existing:
        with table.update_schema() as update:
            update.union_by_name(QUEUE_SCHEMA)
        table = catalog.load_table(f"{QUEUE_NAMESPACE}.{QUEUE_TABLE}")
    return table


def queue_location(catalog=None) -> str:
    """``bucket/prefix/`` of the queue table's files (data + metadata). They hold the keys of
    pending requests by design, so the storage verification of an erasure skips them."""
    location = ensure_queue(catalog).location()
    return location.split("://", 1)[-1].rstrip("/") + "/"


def enqueue(
    subject_key: str,
    *,
    identifier_type: str,
    requested_by: str,
    reason: str,
    scope: str = "full",
    catalog=None,
) -> str:
    """Append one request (pyiceberg, no SQL) and return its opaque id."""
    if scope not in SCOPES:
        raise ValueError(f"scope must be one of {SCOPES}")
    request_id = str(uuid.uuid4())
    now = datetime.now(UTC)
    row = {
        "request_id": request_id,
        "subject_key": subject_key,
        "identifier_type": identifier_type,
        "requested_by": requested_by,
        "reason": reason,
        "requested_at": now,
        "scope": scope,
        "status": "pending",
        "attempts": 0,
        "updated_at": now,
    }
    ensure_queue(catalog).append(pa.Table.from_pylist([row], schema=QUEUE_SCHEMA))
    return request_id
