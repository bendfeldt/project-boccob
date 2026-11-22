import re
from datetime import date

import pytest

from governance.classification import manifest as cm
from governance.config import load_retention
from governance.erasure.executor import audit_digest
from governance.erasure.plan import ErasurePlanError, build_plan, validate_subject_key
from governance.iceberg import retention_session
from governance.retention.runner import expired_landing_objects, plan_retention
from ingestion.framework.spec import load_source
from lakehouse.trino_client import fqn, ident
from tests.integration.storage_scan import object_contains

KEY = "ab" * 32
REQ = "0b6f1c2e-6a4d-4c51-9b7e-2f3a4b5c6d7e"
QUEUE = '(select subject_key from "raw"."governance"."erasure_requests" where request_id = \'' + REQ + "')"


def col(pii=None, **meta):
    m = dict(meta)
    if pii is not None:
        m["pii"] = pii
    return {"config": {"meta": m}}


def model(name, layer, schema, columns, gdpr=None, config=None, row_filter=None, alias=None):
    meta = {"layer": layer}
    if gdpr:
        meta["gdpr"] = gdpr
    if row_filter:
        meta["row_filter"] = row_filter
    return {
        "unique_id": f"model.boccob.{name}",
        "resource_type": "model",
        "name": name,
        "alias": alias or name,
        "database": layer,
        "schema": schema,
        "columns": columns,
        "config": {"materialized": "table", "meta": meta, **(config or {})},
    }


def manifest(*models):
    return {"nodes": {m["unique_id"]: m for m in models}, "sources": {}}


GOOD = manifest(
    model(
        "citizens",
        "clean",
        "borgerportal",
        {
            "citizen_id": col("indirect", pseudonymised=True),
            "cpr_number": col("direct", cpr=True, mask="redact"),
            "is_deleted": col("none"),
            "source_updated_at": col("none"),
        },
        gdpr={
            "subject_column": "citizen_id",
            "erasure": "delete",
            "deregistration": {"flag_column": "is_deleted", "since_column": "source_updated_at", "grace_days": 365},
        },
    ),
    model(
        "payments",
        "clean",
        "borgerportal",
        {
            "citizen_id": col("indirect", pseudonymised=True),
            "case_id": col("indirect"),
            "payment_date": col("none"),
            "_anonymised_at": col("none"),
        },
        gdpr={
            "subject_column": "citizen_id",
            "erasure": "delete",
            "retention_days": 365,
            "retention_column": "payment_date",
            "anonymise_columns": ["case_id"],
        },
    ),
    model(
        "dim_citizen",
        "curated",
        "borgerservice",
        {"citizen_key": col("none"), "person_id": col("indirect", pseudonymised=True)},
        gdpr={"subject_column": "person_id", "erasure": "keystore"},
        config={"materialized": "dimension", "business_key": "person_id", "surrogate_key": "citizen_key"},
    ),
    model(
        "fact_payment",
        "curated",
        "borgerservice",
        {"citizen_key": col("none"), "case_id": col("indirect")},
        gdpr={
            "subject_dimension": "dim_citizen",
            "subject_key_column": "citizen_key",
            "erasure": "anonymise",
            "anonymise_columns": ["case_id"],
        },
    ),
    model(
        "dp",
        "curated",
        "borgerservice",
        {"n": col("none")},
        row_filter={"expression": "n >= 5", "exempt_groups": ["pii-reader"]},
        alias="dp_v1",
    ),
)


# --- classification ----------------------------------------------------------------------------


def test_valid_manifest_has_no_violations():
    assert cm.validate(cm.tables_from_manifest(GOOD)) == []


def test_missing_and_invalid_classification_is_reported():
    bad = manifest(
        model(
            "a",
            "clean",
            "s",
            {"x": col(), "y": col("secret"), "z": col("none", mask="rot13"), "cpr": col("indirect", cpr=True)},
        ),
        model("b", "curated", "s", {"pid": col("indirect", pseudonymised=True)}),
        model("c", None, "s", {"x": col("none")}),
    )
    problems = "\n".join(cm.validate(cm.tables_from_manifest(bad)))
    assert "clean.s.a.x: missing meta.pii" in problems
    assert "invalid pii level 'secret'" in problems
    assert "invalid mask strategy 'rot13'" in problems
    assert "CPR columns must be classified direct or sensitive" in problems
    assert "curated.s.b: holds person-bound data but declares no meta.gdpr.erasure" in problems
    assert "c: model has no meta.layer" in problems


def test_policy_document_shape():
    doc = cm.policy_document(cm.tables_from_manifest(GOOD))
    citizens = doc["tables"]["clean.borgerportal.citizens"]["columns"]
    assert citizens["cpr_number"] == {"pii": "direct", "mask": "redact", "cpr": True}
    assert doc["tables"]["curated.borgerservice.dp_v1"]["row_filter"] == {
        "expression": "n >= 5",
        "exempt_groups": ["pii-reader"],
    }
    assert cm.render(doc) == cm.render(cm.policy_document(cm.tables_from_manifest(GOOD))), "stable output"


def test_live_column_drift_detection():
    tables = cm.tables_from_manifest(GOOD)
    actual = {"clean.borgerportal.citizens": {"citizen_id", "cpr_number", "brand_new"}}
    problems = cm.undocumented_columns(tables, actual)
    assert any("brand_new: column exists in the lakehouse but is not classified" in p for p in problems)
    assert any("is_deleted: classified column does not exist" in p for p in problems)


# --- erasure plan ------------------------------------------------------------------------------


def test_erasure_plan_covers_all_layers_in_safe_order():
    plan = build_plan(REQ, cm.tables_from_manifest(GOOD), [load_source("borgerportal")])
    actions = [(s.table, s.action) for s in plan.steps]
    assert actions[:3] == [
        ("raw.borgerportal.citizens", "raw_index"),
        ("raw.borgerportal.cases", "raw_index"),
        ("raw.borgerportal.payments", "raw_index"),
    ]
    assert ("raw.borgerportal.case_types", "raw_index") not in actions, "no identifiers declared"
    order = [a for _, a in actions]
    # facts are resolved through the keystore before the keystore entry is anonymised
    assert order.index("anonymise") < order.index("delete") < order.index("keystore")
    assert len(plan.raw_file_queries) == 3
    assert "curated.keystore.dim_citizen" in plan.tables


def test_erasure_statements():
    plan = build_plan(REQ, cm.tables_from_manifest(GOOD), [])
    by_table = {s.table: s for s in plan.steps}
    assert by_table["clean.borgerportal.citizens"].statements == (
        f'delete from "clean"."borgerportal"."citizens" where "citizen_id" in {QUEUE}',
    )
    fact = by_table["curated.borgerservice.fact_payment"].statements[0]
    assert fact.startswith(
        'update "curated"."borgerservice"."fact_payment" set "case_id" = null where "citizen_key" in'
    )
    dim = by_table["curated.borgerservice.dim_citizen"].statements
    assert "'erased:' || cast(surrogate_key as varchar)" in dim[0] and dim[1].startswith("delete from")
    assert "member_type = 'erased'" in dim[2], "the integer key is kept, only the business key is removed"
    assert not any('delete from "curated"."keystore"' in s for st in plan.steps for s in st.statements)


@pytest.mark.parametrize("bad", ["", "abc", "AB" * 32, "ab" * 31 + "'; drop table x --", "zz" * 32])
def test_subject_key_is_validated(bad):
    with pytest.raises(ErasurePlanError):
        validate_subject_key(bad)


@pytest.mark.parametrize("bad", ["", KEY, REQ.upper(), REQ + "'; drop table x --", "not-a-uuid"])
def test_request_id_is_validated(bad):
    with pytest.raises(ErasurePlanError):
        build_plan(bad, [], [])


def test_no_subject_key_in_any_sql_text():
    """H-1: SQL text is archived in the WORM audit trail; statements reference the subject only
    through the opaque request id."""
    plan = build_plan(REQ, cm.tables_from_manifest(GOOD), [load_source("borgerportal")])
    sql = [q for _, q in plan.raw_file_queries]
    for step in plan.steps:
        sql += [*step.statements, step.count_sql, step.verify_sql]
    assert sql
    for text in sql:
        assert not re.search(r"[0-9a-f]{64}", text), text
        assert not re.search(r"\d{6}-?\d{4}", text), text
    referencing = [t for t in sql if "erasure_requests" in t]
    assert all(REQ in t for t in referencing) and len(referencing) >= len(plan.steps)


def test_audit_digest_does_not_reveal_the_key():
    digest = audit_digest(KEY, "pepper-pepper-pepper-pepper-pepper-123")
    assert digest != KEY and len(digest) == 64
    assert digest == audit_digest(KEY, "pepper-pepper-pepper-pepper-pepper-123")
    assert digest != audit_digest(KEY, "another-pepper-another-pepper-another-1")


def test_identifier_quoting_rejects_injection():
    assert fqn("curated", "keystore", "dim_citizen") == '"curated"."keystore"."dim_citizen"'
    for bad in ['x"; drop', "a b", "1abc", ""]:
        with pytest.raises(ValueError):
            ident(bad)


# --- storage scan ------------------------------------------------------------------------------


def test_storage_scan_finds_needles_in_parquet_and_text():
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    pq.write_table(pa.table({"k": ["x", KEY], "n": [1, 2]}), buf, compression="zstd")
    assert KEY.encode() not in buf.getvalue() or True  # compressed: byte search alone is not enough
    assert object_contains("a/b.parquet", buf.getvalue(), [KEY])
    assert not object_contains("a/b.parquet", buf.getvalue(), ["cd" * 32])
    assert object_contains("a/file.json", b'{"cpr": "610397-0407"}', ["610397-0407"])
    assert not object_contains("a/file.bin", b"\x00\x01", [KEY])


# --- retention ---------------------------------------------------------------------------------


def test_retention_plan():
    config = load_retention()
    plan = plan_retention(cm.tables_from_manifest(GOOD), [load_source("borgerportal")], config, date(2026, 6, 30))
    by_table = {s.table: s.sql for s in plan.statements}
    assert by_table["clean.borgerportal.payments"] == (
        'delete from "clean"."borgerportal"."payments" where cast("payment_date" as date) < date \'2025-06-30\''
    )
    assert "timestamp '2026-04-01 00:00:00 UTC'" in by_table["raw.borgerportal.citizens"]  # 90 days
    assert "max_by(_load_id" not in by_table["raw.borgerportal.citizens"]
    assert by_table["raw.borgerportal.case_types"].endswith(
        'and _load_id <> (select max_by(_load_id, cast(_load_id as double)) from "raw"."borgerportal"."case_types")'
    ), "the latest reference snapshot is never expired"
    assert plan.landing_cutoff == date(2026, 5, 31)
    assert "clean.borgerportal.citizens" not in plan.skipped, "master data has a deregistration rule"
    (dereg,) = plan.deregistrations
    assert dereg.table == "clean.borgerportal.citizens"
    assert dereg.select_sql == (
        'select s.subject from (select "citizen_id" as subject, max_by("is_deleted", "source_updated_at") as flag, '
        'max("source_updated_at") as since from "clean"."borgerportal"."citizens" group by 1) s '
        "where s.flag and s.since < timestamp '2025-06-30 00:00:00 UTC' "
        'and s.subject not in (select subject_key from "raw"."governance"."erasure_requests")'
    )


def test_deregistration_rule_is_validated():
    bad = manifest(
        model(
            "p",
            "clean",
            "s",
            {"pid": col("indirect", pseudonymised=True), "gone": col("none")},
            gdpr={
                "subject_column": "pid",
                "erasure": "delete",
                "deregistration": {"flag_column": "gone", "since_column": "missing", "grace_days": 1},
            },
        ),
        model(
            "q",
            "clean",
            "s",
            {"pid": col("indirect", pseudonymised=True)},
            gdpr={"subject_column": "pid", "erasure": "delete", "deregistration": {"flag_column": "pid"}},
        ),
    )
    problems = "\n".join(cm.validate(cm.tables_from_manifest(bad)))
    assert "clean.s.p: gdpr.deregistration columns must be documented columns" in problems
    assert "clean.s.q: gdpr.deregistration needs flag_column, since_column, grace_days" in problems


def test_expired_landing_objects():
    keys = [
        "s/e/load_date=2026-01-01/100.1/a.json",  # old, committed
        "s/e/load_date=2026-06-25/200.2/b.json",  # recent, committed
        "s/e/load_date=2026-06-20/300.3/c.json",  # aborted load, older than the uncommitted cutoff
        "s/e/load_date=2026-06-29/400.4/d.json",  # aborted load, still inside the grace period
        "s/_manifest/100.1.x.jsonl",
    ]
    expired = expired_landing_objects(keys, {"100.1", "200.2"}, date(2026, 5, 31), date(2026, 6, 28))
    assert expired == [keys[0], keys[2]]


def test_retention_session_properties():
    assert retention_session(["raw"], "0s") == {
        "raw.expire_snapshots_min_retention": "0s",
        "raw.remove_orphan_files_min_retention": "0s",
    }
    with pytest.raises(ValueError):
        retention_session(["raw"], "0s; drop")
    with pytest.raises(ValueError):
        retention_session(["ra w"])


# --- object versions (C-1) ---------------------------------------------------------------------


def _v(key, vid, minute, latest=False, marker=False):
    from datetime import UTC, datetime

    from lakehouse.objectstore import ObjectVersion

    return ObjectVersion(key, vid, latest, marker, datetime(2026, 1, 1, 0, minute, tzinfo=UTC))


def test_noncurrent_selection_keeps_current_versions():
    from lakehouse.objectstore import noncurrent_versions

    versions = [
        _v("live", "a1", 0),
        _v("live", "a2", 5, latest=True),  # overwritten object
        _v("gone", "b1", 0),
        _v("gone", "m1", 3, latest=True, marker=True),  # deleted object
        _v("only", "c1", 1, latest=True),  # untouched object
        _v("orphan-marker", "m2", 2, latest=True, marker=True),  # marker without data
    ]
    selected = noncurrent_versions(versions)
    ids = [v.version_id for v in selected]
    assert set(ids) == {"a1", "b1", "m1", "m2"}
    assert "a2" not in ids and "c1" not in ids, "current versions are never purged"
    # data versions are removed before delete markers (never re-expose an old version)
    assert ids.index("b1") < ids.index("m1")


def test_noncurrent_selection_respects_age():
    from datetime import UTC, datetime

    from lakehouse.objectstore import noncurrent_versions

    versions = [
        _v("old", "o1", 0),
        _v("old", "om", 1, latest=True, marker=True),  # noncurrent since minute 1
        _v("new", "n1", 0),
        _v("new", "nm", 9, latest=True, marker=True),  # noncurrent since minute 9
    ]
    cutoff = datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
    ids = {v.version_id for v in noncurrent_versions(versions, older_than=cutoff)}
    assert ids == {"o1", "om"}, "versions inside the recovery window stay, with their marker"


def test_deregistration_keeps_bookkeeping_rows_anonymised():
    """N-6 (controller decision): a deregistration erasure removes the person link at once but keeps
    rows of tables with their own retention until it expires; an art. 17 request deletes them."""
    tables = cm.tables_from_manifest(GOOD)
    full = {s.table: s for s in build_plan(REQ, tables, []).steps}
    assert full["clean.borgerportal.payments"].action == "delete"
    dereg = {s.table: s for s in build_plan(REQ, tables, [], scope="deregistration", today=date(2030, 6, 1)).steps}
    payments = dereg["clean.borgerportal.payments"]
    assert payments.action == "retain_anonymised"
    expire, anonymise = payments.statements
    assert expire == (
        f'delete from "clean"."borgerportal"."payments" where "citizen_id" in {QUEUE} '
        "and cast(\"payment_date\" as date) < date '2029-06-01'"
    )
    assert anonymise == (
        'update "clean"."borgerportal"."payments" set "citizen_id" = null, "case_id" = null, '
        f'"_anonymised_at" = current_timestamp where "citizen_id" in {QUEUE}'
    )
    assert payments.verify_sql == payments.count_sql and "citizen_id" in payments.verify_sql
    # master data has no time rule: deleted in both scopes; facts and keystore as for a full erasure
    assert dereg["clean.borgerportal.citizens"].action == "delete"
    assert dereg["curated.borgerservice.fact_payment"].action == "anonymise"
    assert dereg["curated.borgerservice.dim_citizen"].action == "keystore"
    with pytest.raises(ErasurePlanError):
        build_plan(REQ, tables, [], scope="partial")


def test_retained_table_needs_the_anonymised_marker():
    node = model(
        "payments",
        "clean",
        "borgerportal",
        {"citizen_id": col("indirect", pseudonymised=True), "payment_date": col("none")},
        gdpr={
            "subject_column": "citizen_id",
            "erasure": "delete",
            "retention_days": 1,
            "retention_column": "payment_date",
        },
    )
    tables = cm.tables_from_manifest(manifest(node))
    assert any("_anonymised_at" in p for p in cm.validate(tables))
    with pytest.raises(ErasurePlanError, match="_anonymised_at"):
        build_plan(REQ, tables, [], scope="deregistration")


def test_real_project_deregistration_plan():
    """The shipped models: cases and payments are kept anonymised, everything else as before."""
    from governance.config import manifest_path

    path = manifest_path()
    if not path.is_file():
        pytest.skip("dbt manifest missing - run `make dbt-parse`")
    tables = cm.tables_from_manifest(cm.load_manifest(path))
    steps = {s.table: s.action for s in build_plan(REQ, tables, [], scope="deregistration").steps}
    assert steps["clean.borgerportal.cases"] == steps["clean.borgerportal.payments"] == "retain_anonymised"
    assert steps["clean.borgerportal.citizens"] == steps["consolidated.party.person"] == "delete"
