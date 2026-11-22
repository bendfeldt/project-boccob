"""Erasure executor control flow with fakes (no Trino, no object store): requests are never lost,
one failure does not stop the batch, each table is purged once per batch, the audit row is
asserted, dry runs do not write."""

from __future__ import annotations

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
    def __init__(self, pending=(), audit_rows=1, remaining=()):
        self.pending = list(pending)
        self.audit_rows = audit_rows
        self.remaining = set(remaining)  # request ids whose subject is still found after the purge
        self.statements: list[str] = []
        self.params: list = []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        self.params.append(params)
        if "information_schema" in sql:
            return []
        if "$metadata_log_entries" in sql:
            return [["s3://lakehouse-clean/iceberg/s/t-1234/metadata/00001-abc.metadata.json"]]
        if sql.startswith("insert into") and "erasure_log" in sql:
            return [[self.audit_rows]]
        if sql.startswith("select request_id"):
            return [list(p) for p in self.pending if f"'{p[0]}'" in sql or "where request_id" not in sql]
        if sql.startswith("select count(*)") and any(r in sql for r in self.remaining):
            return [[1]]
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
    def plan(request_id, tables, sources, **kwargs):
        sub = f"select subject_key from q where request_id = '{request_id}'"
        return ErasurePlan(
            request_id=request_id,
            steps=[
                Step(
                    table=table,
                    action="delete",
                    statements=(f"delete from {table} where k in ({sub})",),
                    count_sql=f"select count(*) from {table} where k in ({sub})",
                    verify_sql=f"select count(*) from {table} where k in ({sub})",
                )
                for table in ("clean.s.t", "clean.s.u")
            ],
        )

    monkeypatch.setattr(ex, "build_plan", plan)
    monkeypatch.setattr(ex, "list_sources", list)
    monkeypatch.setattr(ex.cm, "load_manifest", lambda path: {})
    monkeypatch.setattr(ex.cm, "tables_from_manifest", lambda manifest: [])
    monkeypatch.setattr(ex, "pseudonym_pepper", lambda: "p" * 32)
    monkeypatch.setattr(ex.rq, "ensure_queue", lambda *a, **k: None)
    monkeypatch.setattr(ex.rq, "queue_location", lambda *a, **k: QUEUE_LOCATION)
    histories: list[str] = []
    monkeypatch.setattr(ex.iceberg, "purge_history", lambda trino, ref: histories.append(str(ref)))
    purges: list[tuple] = []
    monkeypatch.setattr(ex, "purge_noncurrent", lambda fs, bucket, prefix="", **k: purges.append((bucket, prefix)) or 0)
    monkeypatch.setattr(ex, "delete_permanently", lambda *a, **k: 0)
    return {"purges": purges, "histories": histories}


def pending(*rows, scope="full"):
    return [(req, key, "subject_key", "dpo", "ticket", scope) for req, key in rows]


def run(trino):
    return ex.process_pending_requests(trino=trino, s3=FakeS3(), purge_s3=FakeS3())


def test_batch_purges_each_table_once_and_dequeues_after_the_audit(env):
    trino = FakeTrino(pending((REQ_A, KEY_A), (REQ_B, KEY_B)))
    reports = run(trino)
    assert [(r.request_id, r.status) for r in reports] == [(REQ_A, "completed"), (REQ_B, "completed")]
    # two requests, two tables: one history purge per table, then the queue's own history
    assert env["histories"] == ["clean.s.t", "clean.s.u", "raw.governance.erasure_requests"]
    assert env["purges"].count(("lakehouse-raw", "iceberg/governance/erasure_requests/")) == 1
    audits = [i for i, s in enumerate(trino.statements) if s.startswith("insert") and "erasure_log" in s]
    dequeues = [i for i, s in enumerate(trino.statements) if s.startswith("delete from") and "erasure_requests" in s]
    assert len(audits) == 2 and audits[0] < dequeues[0] and audits[1] < dequeues[1], "audit row first"
    # the deletes of both requests run before the (single) purge
    deletes = [i for i, s in enumerate(trino.statements) if s.startswith("delete from clean.s.t")]
    assert len(deletes) == 2 and deletes[1] < audits[0]
    # the keyed digest is a parameter of the audit insert; the subject key never reaches SQL
    digests = [p[1] for s, p in zip(trino.statements, trino.params, strict=True) if "erasure_log" in s and p]
    assert digests == [ex.audit_digest(KEY_A, "p" * 32), ex.audit_digest(KEY_B, "p" * 32)]
    assert not any(KEY_A in s or KEY_B in s for s in trino.statements)


def test_late_failure_keeps_the_request_and_the_batch_continues(env):
    trino = FakeTrino(pending((REQ_A, KEY_A), (REQ_B, KEY_B)), remaining=[REQ_A])
    with pytest.raises(ex.ErasureBatchError) as caught:
        run(trino)
    first, second = caught.value.reports
    assert (first.request_id, first.status) == (REQ_A, "failed") and "still present" in first.error
    assert (second.request_id, second.status) == (REQ_B, "completed"), "one failure must not stop the queue"
    assert not trino.dequeued(REQ_A) and trino.marked_failed(REQ_A)
    assert trino.dequeued(REQ_B) and not trino.marked_failed(REQ_B)
    assert len([s for s in trino.statements if s.startswith("insert") and "erasure_log" in s]) == 2, "failure audited"


def test_failed_purge_keeps_every_request_of_the_batch(env, monkeypatch):
    def broken(trino, ref):
        raise RuntimeError("optimize failed")

    monkeypatch.setattr(ex.iceberg, "purge_history", broken)
    trino = FakeTrino(pending((REQ_A, KEY_A), (REQ_B, KEY_B)))
    with pytest.raises(ex.ErasureBatchError):
        run(trino)
    for req in (REQ_A, REQ_B):
        assert not trino.dequeued(req) and trino.marked_failed(req)


def test_audit_row_must_be_written_exactly_once(env):
    trino = FakeTrino(pending((REQ_A, KEY_A)), audit_rows=0)
    with pytest.raises(ex.ErasureBatchError) as caught:
        run(trino)
    (report,) = caught.value.reports
    assert report.status == "failed" and "ErasureAuditError" in report.error
    assert not trino.dequeued(REQ_A) and trino.marked_failed(REQ_A)


def test_dry_run_lists_the_plan_without_any_query(monkeypatch):
    """The dry run is the plan itself: no identifier, no query, nothing written."""
    from governance.classification import manifest as cm
    from ingestion.framework.spec import load_source
    from tests.unit.test_governance import GOOD

    monkeypatch.setattr(ex.cm, "load_manifest", lambda path: GOOD)
    monkeypatch.setattr(ex, "list_sources", lambda: [load_source("borgerportal")])
    monkeypatch.setattr(ex.rq, "enqueue", lambda *a, **k: pytest.fail("a dry run must not write the queue"))
    monkeypatch.setattr(ex, "TrinoClient", lambda *a, **k: pytest.fail("a dry run needs no Trino"))
    report = ex.preview()
    assert report.dry_run and report.status == "planned" and len(cm.tables_from_manifest(GOOD)) > 0
    assert "clean.borgerportal.citizens [delete]" in report.planned
    assert "curated.borgerservice.fact_payment [anonymise]" in report.planned
    dereg = ex.preview(scope="deregistration").planned
    assert dereg != report.planned, "the dry run follows the scope (deregistration keeps bookings)"


def test_cli_only_queues_outside_dev(monkeypatch):
    """Executing outside the writer pool could race a dbt MERGE: outside dev the CLI only queues."""
    from governance.erasure import cli

    monkeypatch.setenv("LAKEHOUSE_ENV", "prod")
    monkeypatch.setenv("ERASURE_IDENTIFIER", "0101011234")
    monkeypatch.setattr(ex, "erase_subject", lambda **k: pytest.fail("must not execute outside dev"))
    monkeypatch.setattr(ex, "process_pending_requests", lambda **k: pytest.fail("must not execute outside dev"))
    queued = []
    monkeypatch.setattr(ex, "submit_request", lambda **k: queued.append(k) or REQ_A)
    assert cli.main(["--requested-by", "dpo", "--reason", "ticket"]) == 0 and len(queued) == 1
    with pytest.raises(SystemExit) as caught:
        cli.main(["--process-pending"])
    assert caught.value.code == 2
