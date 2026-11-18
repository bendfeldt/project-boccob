"""Erasure executor control flow with fakes (no Trino, no object store): requests are never lost,
one failure does not stop the batch, the audit row is asserted, dry runs do not write."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from governance.erasure import executor as ex
from governance.erasure.plan import ErasurePlan, Step

KEY_A, KEY_B = "aa" * 32, "bb" * 32
REQ_A = "0b6f1c2e-6a4d-4c51-9b7e-2f3a4b5c6d7a"
REQ_B = "0b6f1c2e-6a4d-4c51-9b7e-2f3a4b5c6d7b"
QUEUE_LOCATION = "lakehouse-raw/iceberg/governance/erasure_requests/"


class FakeFS:
    def pipe_file(self, path, data):
        pass


class FakeS3:
    raw_bucket = "lakehouse-raw"

    def filesystem(self):
        return FakeFS()


class FakeTrino:
    def __init__(self, pending=(), audit_rows=1):
        self.pending = list(pending)
        self.audit_rows = audit_rows
        self.statements: list[str] = []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if "information_schema" in sql:
            return []
        if sql.startswith("insert into") and "erasure_log" in sql:
            return [[self.audit_rows]]
        if sql.startswith("select request_id"):
            return [list(p) for p in self.pending]
        return [[0]]

    def scalar(self, sql, params=None):
        rows = self.execute(sql, params)
        return rows[0][0] if rows else None

    def table_exists(self, *parts):
        return True

    def dequeued(self, request_id):
        return any(s.startswith("delete from") and "erasure_requests" in s and request_id in s for s in self.statements)

    def marked_failed(self, request_id):
        return any("set status = 'failed'" in s and request_id in s for s in self.statements)


@pytest.fixture
def env(monkeypatch):
    scans: list[dict] = []
    hits: dict[str, list[str]] = {}

    def plan(request_id, tables, sources, **kwargs):
        sub = f"select subject_key from q where request_id = '{request_id}'"
        return ErasurePlan(
            request_id=request_id,
            steps=[
                Step(
                    table="clean.s.t",
                    action="delete",
                    statements=(f"delete from t where k in ({sub})",),
                    count_sql=f"select count(*) from t where k in ({sub})",
                    verify_sql=f"select count(*) from t where k in ({sub})",
                )
            ],
        )

    def scan(fs, buckets, needles, exclude=None):
        scans.append({"needles": needles, "exclude": exclude})
        return hits.get(needles[0], [])

    monkeypatch.setattr(ex, "build_plan", plan)
    monkeypatch.setattr(ex, "list_sources", list)
    monkeypatch.setattr(ex.cm, "load_manifest", lambda path: {})
    monkeypatch.setattr(ex.cm, "tables_from_manifest", lambda manifest: [])
    monkeypatch.setattr(ex, "pseudonym_pepper", lambda: "p" * 32)
    monkeypatch.setattr(ex.rq, "stage_digest", lambda *a, **k: None)
    monkeypatch.setattr(ex.rq, "ensure_queue", lambda *a, **k: None)
    monkeypatch.setattr(ex.rq, "queue_location", lambda *a, **k: QUEUE_LOCATION)
    monkeypatch.setattr(ex.rq, "enqueue", lambda *a, **k: pytest.fail("a dry run must not write the queue"))
    monkeypatch.setattr(ex.iceberg, "purge_history", lambda *a, **k: None)
    monkeypatch.setattr(ex, "purge_noncurrent", lambda *a, **k: 0)
    monkeypatch.setattr(ex, "delete_permanently", lambda *a, **k: 0)
    monkeypatch.setattr(ex, "scan_storage", scan)
    return {"scans": scans, "hits": hits}


def pending(*rows):
    return [(req, key, "subject_key", "dpo", "ticket", "full") for req, key in rows]


def run(trino):
    return ex.process_pending_requests(verify_storage=True, trino=trino, s3=FakeS3(), purge_s3=FakeS3())


def test_successful_request_is_dequeued_only_at_the_end(env):
    trino = FakeTrino(pending((REQ_A, KEY_A)))
    (report,) = run(trino)
    assert report.status == "completed" and report.request_id == REQ_A
    assert trino.dequeued(REQ_A) and not trino.marked_failed(REQ_A)
    audit = next(i for i, s in enumerate(trino.statements) if "erasure_log" in s and s.startswith("insert"))
    dequeue = next(i for i, s in enumerate(trino.statements) if s.startswith("delete from") and "erasure_requests" in s)
    assert audit < dequeue, "the request leaves the queue only after the audit row exists"
    # the storage scan skips the queue's own files: they still hold the key at that point
    assert env["scans"] == [{"needles": [KEY_A], "exclude": [QUEUE_LOCATION]}]


def test_late_failure_keeps_the_request_and_the_batch_continues(env):
    env["hits"][KEY_A] = ["s3://lakehouse-clean/x.parquet?versionId=1"]  # found after the purge
    trino = FakeTrino(pending((REQ_A, KEY_A), (REQ_B, KEY_B)))
    with pytest.raises(ex.ErasureBatchError) as caught:
        run(trino)
    first, second = caught.value.reports
    assert (first.request_id, first.status) == (REQ_A, "failed") and "still present in objects" in first.error
    assert (second.request_id, second.status) == (REQ_B, "completed"), "one failure must not stop the queue"
    assert not trino.dequeued(REQ_A) and trino.marked_failed(REQ_A)
    assert trino.dequeued(REQ_B) and not trino.marked_failed(REQ_B)
    # the failure is audited as well (with the staged digest, never the key)
    audits = [s for s in trino.statements if s.startswith("insert") and "erasure_log" in s]
    assert len(audits) == 2 and not any(KEY_A in s or KEY_B in s for s in trino.statements)


def test_audit_row_must_be_written_exactly_once(env):
    trino = FakeTrino(pending((REQ_A, KEY_A)), audit_rows=0)
    with pytest.raises(ex.ErasureBatchError) as caught:
        run(trino)
    (report,) = caught.value.reports
    assert report.status == "failed" and "ErasureAuditError" in report.error
    assert not trino.dequeued(REQ_A) and trino.marked_failed(REQ_A)


def test_storage_scan_skips_excluded_prefixes(monkeypatch):
    from governance.erasure import storage_scan
    from lakehouse import objectstore

    now = datetime.now(UTC)
    versions = [
        objectstore.ObjectVersion("iceberg/governance/erasure_requests/data/a.parquet", "1", True, False, now),
        objectstore.ObjectVersion("iceberg/clean/t/data/b.parquet", "1", True, False, now),
    ]
    monkeypatch.setattr(objectstore, "iter_versions", lambda fs, bucket, prefix="": iter(versions))
    monkeypatch.setattr(objectstore, "read_version", lambda fs, bucket, v: b"... needle ...")
    hits = storage_scan.scan_storage(None, ["lakehouse-raw"], ["needle"], exclude=[QUEUE_LOCATION])
    assert hits == ["s3://lakehouse-raw/iceberg/clean/t/data/b.parquet?versionId=1"]
