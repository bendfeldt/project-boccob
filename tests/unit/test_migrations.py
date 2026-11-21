"""Versioned schema migrations (audit N-5): parsing, ordering, recorded history, drift."""

from __future__ import annotations

import pytest

from governance.migrations import runner as mr


def write(tmp_path, name, text):
    (tmp_path / name).write_text(text, encoding="utf-8")


def test_repository_migrations_parse_and_are_idempotent():
    migrations = mr.discover()
    assert [m.label for m in migrations][:1] == ["V0001__clean_citizens_drop_cpr_number"]
    first = migrations[0]
    assert first.statements == ('alter table if exists "clean".borgerportal.citizens drop column if exists cpr_number',)
    assert first.purge_history == (("clean", "borgerportal", "citizens"),)
    for m in migrations:  # every statement must tolerate a new lake (no table yet) and a re-run
        for statement in m.statements:
            assert " if exists " in statement.lower() or " if not exists " in statement.lower(), statement


def test_placeholders_follow_the_catalog_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("CLEAN_CATALOG", "clean_test")
    write(tmp_path, "V0001__a.sql", "alter table if exists ${clean}.s.t drop column if exists c;\nselect 1 from x;\n")
    (m,) = mr.discover(tmp_path)
    assert m.statements == ('alter table if exists "clean_test".s.t drop column if exists c', "select 1 from x")


@pytest.mark.parametrize(
    ("name", "text", "message"),
    [
        ("V1__a.sql", "select 1;", "named V<nnnn>"),
        ("V0001__a.sql", "select 1 from ${nope}.s.t;", "unknown placeholder"),
        ("V0001__a.sql", "delete from t where k = '" + "ab" * 32 + "';", "key-like"),
        ("V0001__a.sql", "delete from t where cpr = '010101-1234';", "key-like or CPR"),
        ("V0001__a.sql", "-- @purge-history nolayer.s.t\n", "layer"),
        ("V0001__a.sql", "-- only a comment\n", "no statement"),
    ],
)
def test_invalid_migrations_are_rejected(tmp_path, name, text, message):
    write(tmp_path, name, text)
    with pytest.raises(mr.MigrationError, match=message):
        mr.discover(tmp_path)


def test_duplicate_versions_are_rejected(tmp_path):
    write(tmp_path, "V0001__a.sql", "select 1;")
    write(tmp_path, "V0001__b.sql", "select 2;")
    with pytest.raises(mr.MigrationError, match="duplicate"):
        mr.discover(tmp_path)


def test_recorded_history_is_checked(tmp_path):
    write(tmp_path, "V0001__a.sql", "select 1;")
    write(tmp_path, "V0002__b.sql", "select 2;")
    first, second = mr.discover(tmp_path)
    assert mr.check([first, second], {}) == [first, second]
    assert mr.check([first, second], {"0001": ("a", first.checksum)}) == [second]
    with pytest.raises(mr.MigrationError, match="changed after it was applied"):
        mr.check([first, second], {"0001": ("a", "0" * 64)})
    with pytest.raises(mr.MigrationError, match="newer release"):
        mr.check([first], {"0001": ("a", first.checksum), "0002": ("b", second.checksum)})


class FakeTrino:
    def __init__(self, applied=(), columns=(), insert_rows=1, locks=()):
        self.applied = list(applied)
        self.columns = list(columns)
        self.insert_rows = insert_rows
        self.locks = list(locks)  # (token, holder, acquired_at)
        self.statements: list[str] = []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if "schema_migration_lock" in sql:
            if sql.startswith("insert"):
                self.locks.append((params[0], params[1], "9999"))
            elif sql.startswith("select"):
                return [list(r) for r in sorted(self.locks, key=lambda r: (r[2], r[0]))]
            elif sql.startswith("delete") and params:
                self.locks = [r for r in self.locks if r[0] != params[0]]
            return [[1]]
        if sql.startswith("select version"):
            return [list(a) for a in self.applied]
        if sql.startswith("insert into"):
            self.applied.append(tuple(params[:3]))
            return [[self.insert_rows]]
        if "information_schema.columns" in sql:
            return [list(c) if isinstance(c, tuple) else [c, "varchar"] for c in self.columns]
        return [[True]]

    def scalar(self, sql, params=None):
        rows = self.execute(sql, params)
        return rows[0][0] if rows else None

    def table_exists(self, *parts):
        return False


def test_run_applies_pending_in_order_once(tmp_path):
    write(tmp_path, "V0002__b.sql", "alter table if exists ${clean}.s.t add column if not exists y varchar;")
    write(
        tmp_path,
        "V0001__a.sql",
        "alter table if exists ${clean}.s.t drop column if exists x;\n-- @purge-history clean.s.t",
    )
    trino = FakeTrino()
    report = mr.run_migrations(directory=tmp_path, trino=trino, check_drift=False)
    assert report.applied == ["V0001__a", "V0002__b"] and report.purged == [], "missing table: nothing to purge"
    alters = [s for s in trino.statements if s.startswith("alter")]
    assert "drop column" in alters[0] and "add column" in alters[1]
    again = mr.run_migrations(directory=tmp_path, trino=trino, check_drift=False)
    assert again.applied == [] and again.already_applied == ["V0001__a", "V0002__b"]
    dry = mr.run_migrations(directory=tmp_path, trino=FakeTrino(), dry_run=True)
    assert dry.pending == ["V0001__a", "V0002__b"] and dry.applied == []


def test_record_row_is_asserted(tmp_path):
    write(tmp_path, "V0001__a.sql", "select 1;")
    with pytest.raises(mr.MigrationError, match="record row"):
        mr.run_migrations(directory=tmp_path, trino=FakeTrino(insert_rows=0), check_drift=False)


def test_drift_check_flags_columns_the_model_dropped():
    from governance.classification import manifest as cm
    from tests.unit.test_governance import GOOD, manifest, model

    tables = cm.tables_from_manifest(GOOD)
    trino = FakeTrino(columns=["citizen_id", "cpr_number", "is_deleted"])
    incremental = [t for t in tables if t.name == "citizens"]
    assert mr.schema_drift(trino, incremental) == [], "GOOD declares cpr_number, but it is a table model"
    node = model("citizens", "clean", "borgerportal", {"citizen_id": {"config": {"meta": {"pii": "indirect"}}}})
    node["config"]["materialized"] = "incremental"
    (t,) = cm.tables_from_manifest(manifest(node))
    problems = mr.schema_drift(trino, [t])
    assert problems == [
        "clean.borgerportal.citizens.cpr_number: in the lake but not in the model - ship a migration",
        "clean.borgerportal.citizens.is_deleted: in the lake but not in the model - ship a migration",
    ]


def test_purge_history_rewrites_every_row_first(tmp_path, monkeypatch):
    """A single data file without deletes would survive OPTIMIZE with the dropped column's values."""
    write(
        tmp_path,
        "V0001__a.sql",
        "alter table if exists ${clean}.s.t drop column if exists x;\n-- @purge-history clean.s.t",
    )
    calls = []
    monkeypatch.setattr(mr.iceberg, "purge_history", lambda trino, ref: calls.append(("purge", str(ref))))

    class Existing(FakeTrino):
        def table_exists(self, *parts):
            return True

        def execute(self, sql, params=None):
            if "information_schema.columns" in sql and "ordinal_position" in sql:
                self.statements.append(sql)
                return [["citizen_id"]]
            if sql.startswith("update"):
                calls.append(("update", sql))
            return super().execute(sql, params)

    report = mr.run_migrations(directory=tmp_path, trino=Existing(), check_drift=False)
    assert report.purged == ["clean.s.t"]
    assert calls == [("update", 'update "clean"."s"."t" set "citizen_id" = "citizen_id"'), ("purge", "clean.s.t")]


def test_drift_check_flags_declared_type_changes():
    from governance.classification import manifest as cm
    from tests.unit.test_governance import manifest, model

    node = model(
        "payments",
        "clean",
        "borgerportal",
        {
            "amount": {"data_type": "decimal(18, 2)", "config": {"meta": {"pii": "indirect"}}},
            "status": {"data_type": "VARCHAR", "config": {"meta": {"pii": "none"}}},
            "note": {"config": {"meta": {"pii": "none"}}},
        },
    )
    node["config"]["materialized"] = "incremental"
    (t,) = cm.tables_from_manifest(manifest(node))
    trino = FakeTrino(columns=[("amount", "double"), ("status", "varchar"), ("note", "bigint")])
    assert mr.schema_drift(trino, [t]) == [
        "clean.borgerportal.payments.amount: type double in the lake, decimal(18, 2) in the model - ship a migration"
    ], "undeclared types are left to the dbt guard; declared ones are compared case/space-insensitively"


def test_checksum_ignores_comments_and_whitespace_but_not_statements(tmp_path):
    a, b, c, d = (tmp_path / x for x in "abcd")
    for directory in (a, b, c, d):
        directory.mkdir()
    write(
        a,
        "V0001__x.sql",
        "-- why\nalter table if exists ${clean}.s.t drop column if exists x;\n-- @purge-history clean.s.t\n",
    )
    write(
        b,
        "V0001__x.sql",
        "-- a much better explanation\n\nalter table if exists ${clean}.s.t\n    drop column if exists x;  -- note\n"
        "-- @purge-history clean.s.t\n",
    )
    write(
        c, "V0001__x.sql", "alter table if exists ${clean}.s.t drop column if exists y;\n-- @purge-history clean.s.t\n"
    )
    write(d, "V0001__x.sql", "alter table if exists ${clean}.s.t drop column if exists x;\n")
    (ma,), (mb,), (mc,), (md,) = (mr.discover(x) for x in (a, b, c, d))
    assert ma.checksum == mb.checksum, "comment / whitespace edits do not change the checksum"
    assert mc.checksum != ma.checksum, "a changed statement does"
    assert md.checksum != ma.checksum, "a removed directive does"
    assert mr.check([mb], {"0001": ("x", ma.checksum)}) == []
    # lakes migrated before the normalisation recorded the raw-file checksum: still accepted
    assert mr.check([ma], {"0001": ("x", ma.legacy_checksum)}) == []
    with pytest.raises(mr.MigrationError):
        mr.check([mb], {"0001": ("x", ma.legacy_checksum)})  # raw text of another file version


def test_inline_comment_outside_literals_only():
    assert mr._strip_inline_comment("select '--not a comment' -- comment") == "select '--not a comment'"


def test_lock_is_taken_and_released(tmp_path):
    write(tmp_path, "V0001__a.sql", "select 1;")
    trino = FakeTrino()
    assert mr.run_migrations(directory=tmp_path, trino=trino, check_drift=False, holder="cli").applied == ["V0001__a"]
    inserts = [s for s in trino.statements if s.startswith("insert") and "schema_migration_lock" in s]
    assert len(inserts) == 1 and trino.locks == [], "lock row removed afterwards"
    # nothing pending: no lock at all (the daily jobs stay cheap)
    again = FakeTrino(applied=trino.applied)
    mr.run_migrations(directory=tmp_path, trino=again, check_drift=False)
    assert not [s for s in again.statements if "schema_migration_lock" in s]


def test_concurrent_runner_is_refused(tmp_path):
    write(tmp_path, "V0001__a.sql", "select 1;")
    trino = FakeTrino(locks=[("other", "dagster run 42", "0000")])  # older live lock
    with pytest.raises(mr.MigrationLockError, match="dagster run 42"):
        mr.run_migrations(directory=tmp_path, trino=trino, check_drift=False, holder="cli")
    assert trino.locks == [("other", "dagster run 42", "0000")], "own row withdrawn, the holder's kept"
    assert not [s for s in trino.statements if s == "select 1"], "nothing applied"


def test_cli_refuses_to_apply_outside_dev_without_confirmation(monkeypatch):
    from governance.migrations import cli

    monkeypatch.setenv("LAKEHOUSE_ENV", "prod")
    monkeypatch.setattr(cli, "run_migrations", lambda **k: pytest.fail("must not apply"))
    with pytest.raises(SystemExit) as caught:
        cli.main([])
    assert caught.value.code == 2

    pending = mr.MigrationReport(pending=["V0001__a"])
    monkeypatch.setattr(cli, "run_migrations", lambda **k: pending)
    assert cli.main(["--check"]) == cli.EXIT_PENDING
