"""End-to-end test on the podman harness (see harness.sh / scripts/integration-test.sh).

The tests run in file order and build on each other:
initial load with the previous release's model shape -> release upgrade (schema migrations) with a
changed load -> dimension rebuild -> idempotent re-run -> access control -> right to erasure ->
another load -> retention and maintenance.
"""

from __future__ import annotations

import itertools
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
os.environ.setdefault("DBT_PROJECT_DIR", str(ROOT / "transform"))
os.environ.setdefault("DBT_PROFILES_DIR", str(ROOT / "transform"))
os.environ["ERASURE_VERIFY_STORAGE"] = "true"

if not os.environ.get("TRINO_HOST"):
    pytest.skip("integration harness environment not loaded (make integration)", allow_module_level=True)

from trino import exceptions as trino_errors  # noqa: E402

from governance.erasure.executor import erase_subject, submit_request  # noqa: E402
from governance.erasure.queue import queue_table_sql  # noqa: E402
from governance.erasure.storage_scan import object_contains, scan_storage  # noqa: E402
from ingestion.framework.manifest import read_manifests, sha256_hex  # noqa: E402
from ingestion.framework.pseudonym import pseudonymise  # noqa: E402
from ingestion.framework.spec import load_source  # noqa: E402
from ingestion.sources.borgerportal import generator  # noqa: E402
from lakehouse.objectstore import iter_versions, read_version  # noqa: E402
from lakehouse.settings import S3Settings, pseudonym_pepper  # noqa: E402
from lakehouse.trino_client import TrinoClient  # noqa: E402

SOURCE = load_source("borgerportal")
SEED, CITIZENS = int(SOURCE.options["seed"]), int(SOURCE.options["citizens"])
BUCKETS = ["lakehouse-raw", "lakehouse-clean", "lakehouse-consolidated", "lakehouse-curated"]
LAYOUT = re.compile(r"^borgerportal/(\w+)/load_date=\d{4}-\d{2}-\d{2}/\d+\.\d+/[\w.]+$")
state: dict = {}


# --- helpers -----------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def trino():
    return TrinoClient()  # svc-dagster: data-engineer + pii-reader


@pytest.fixture(scope="module")
def s3():
    return S3Settings.from_env()


@pytest.fixture(scope="module")
def fs(s3):
    return s3.filesystem()


def run_job(name: str, partition_key: str | None = None, run_config: dict | None = None):
    from orchestration.definitions import defs

    result = defs.resolve_job_def(name).execute_in_process(
        partition_key=partition_key, run_config=run_config, raise_on_error=False
    )
    failures = [str(e.event_specific_data)[:2000] for e in result.all_events if e.is_failure]
    assert result.success, f"job {name} failed: {failures}"
    return result


def dbt(*args: str, env: dict | None = None, ok: bool = True) -> str:
    proc = subprocess.run(
        ["uv", "run", "--no-sync", "dbt", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, **(env or {})},
    )
    assert (proc.returncode == 0) == ok, proc.stdout[-4000:] + proc.stderr[-2000:]
    return proc.stdout


def previous_release_project(target: Path) -> Path:
    """The dbt project as release 0.1 shipped it: clean.citizens still carried the CPR number,
    incremental models used on_schema_change='fail', no _anonymised_at marker. Built from the
    current project so that everything else (keystore macros, other models) is identical."""
    project = target / "transform-previous"
    shutil.copytree(
        ROOT / "transform", project, ignore=shutil.ignore_patterns("target", "logs", "dbt_packages", "migrations")
    )
    for sql in (project / "models").rglob("*.sql"):
        text = sql.read_text(encoding="utf-8")
        text = text.replace("on_schema_change='append_new_columns'", "on_schema_change='fail'")
        text = "".join(line for line in text.splitlines(keepends=True) if "_anonymised_at" not in line)
        if sql.name == "citizens.sql":
            text = text.replace(
                "    {{ subject_key('cpr') }} as citizen_id,\n",
                "    {{ subject_key('cpr') }} as citizen_id,\n    {{ json_text('$.cpr') }} as cpr_number,\n",
            ).replace("  citizen_id,\n  first_name,", "  citizen_id,\n  cpr_number,\n  first_name,")
            assert text.count("cpr_number") == 2
        sql.write_text(text, encoding="utf-8")
    return project


def clean_columns(trino: TrinoClient, table: str) -> set[str]:
    rows = trino.execute(
        "select column_name from clean.information_schema.columns where table_schema = 'borgerportal' "
        "and table_name = ?",
        [table],
    )
    return {r[0] for r in rows}


def keystore(trino: TrinoClient, dimension: str) -> dict[int, dict]:
    rows = trino.execute(
        "select surrogate_key, business_key, version_key, member_type, key_created_at, "
        f"key_last_used_in_fact_at, adopted_at from curated.keystore.{dimension}"
    )
    cols = ("sk", "bk", "vk", "type", "created", "last_used", "adopted")
    return {r[0]: dict(zip(cols, r, strict=True)) for r in rows}


def citizen_dim(trino: TrinoClient) -> dict[tuple, int]:
    rows = trino.execute("select person_id, valid_from, citizen_key from curated.borgerservice.dim_citizen")
    return {(r[0], r[1]): r[2] for r in rows}


def fact_keys(trino: TrinoClient) -> dict[str, tuple]:
    rows = trino.execute("select payment_id, citizen_key, case_type_key from curated.borgerservice.fact_payment")
    return {r[0]: (r[1], r[2]) for r in rows}


def cpr_by_key(deliveries: int = 3) -> dict[str, str]:
    """Pseudonym -> CPR for every person the generator delivered (clean holds no CPR any more)."""
    pepper = pseudonym_pepper()
    out = {}
    for seq in range(1, deliveries + 1):
        batch = generator.simulate(SEED, seq, CITIZENS)
        for rec in batch.citizens + batch.cases + batch.payments:
            out[pseudonymise(rec["cpr"], pepper)] = rec["cpr"]
    return out


def noncurrent_hits(fs, buckets: list[str], needles: list[str]) -> list[str]:
    """Object versions that are no longer current (deleted or overwritten) and contain a needle."""
    hits = []
    for bucket in buckets:
        for v in iter_versions(fs, bucket):
            if (
                not v.is_latest
                and not v.is_delete_marker
                and object_contains(v.key, read_version(fs, bucket, v), needles)
            ):
                hits.append(f"s3://{bucket}/{v.key}?versionId={v.version_id}")
    return hits


def erasure_query_texts(trino: TrinoClient) -> list[str]:
    """Query texts of the governance tooling, read as the auditor (svc-dagster may not see them)."""
    auditor = TrinoClient(user="axel-auditor")
    return [
        r[0]
        for r in auditor.execute(
            "select query from system.runtime.queries where source in ('lakehouse-erasure', 'lakehouse-retention')"
        )
    ]


def assert_dense_keys(ks: dict[int, dict]) -> None:
    positive = sorted(k for k in ks if k > 0)
    assert positive == list(range(1, len(positive) + 1)), "surrogate keys must be sequential without gaps"
    assert ks[-1]["type"] == "unknown"
    pairs = [(v["bk"], v["vk"]) for v in ks.values()]
    assert len(set(pairs)) == len(pairs), "duplicate (business key, version) in keystore"


# --- 1. initial load ---------------------------------------------------------------------------


def test_01_initial_load_with_the_previous_release(trino, fs, s3, tmp_path_factory):
    """The lake is built by the previous release (old model shape, no migration table)."""
    run_job("ingest_borgerportal", partition_key="2026-11-01")
    previous = previous_release_project(tmp_path_factory.mktemp("release-0.1"))
    out = dbt("build", "--project-dir", str(previous))
    assert "ERROR=0" in out
    assert "cpr_number" in clean_columns(trino, "citizens"), "sanity: the old shape carries the CPR"

    # LAND: contract layout, manifest checksums, native bytes untouched
    loads = read_manifests(fs, s3.raw_bucket, "borgerportal")
    assert len(loads) == 1
    ((load_id, entries),) = loads.items()
    assert {e.entity for e in entries} == {"citizens", "cases", "payments", "case_types"}
    batch = generator.simulate(SEED, 1, CITIZENS)
    native = {
        "citizens": generator.citizens_json(batch),
        "cases": generator.cases_xml(batch),
        "payments": generator.payments_csv(batch),
        "case_types": generator.case_types_parquet(batch),
    }
    for e in entries:
        assert LAYOUT.match(e.path), e.path
        content = fs.cat_file(f"{s3.raw_bucket}/{e.path}")
        assert sha256_hex(content) == e.sha256 and len(content) == e.size_bytes
        assert content == native[e.entity], f"{e.entity}: landed file differs from what the source delivered"
        # INDEX: one row per source record, tied to file and load
        rows = trino.execute(
            f"select count(*), count(distinct _raw_row), min(_load_id), min(_raw_sha256) "
            f"from raw.borgerportal.{e.entity} where _raw_file = ?",
            [e.path],
        )
        assert rows[0] == [e.record_count, e.record_count, load_id, e.sha256]

    # CLEAN: typed, deduplicated, pseudonymised
    expected_versions = {(c["cpr"], c["updated_at"]) for c in batch.citizens}
    assert trino.scalar("select count(*) from clean.borgerportal.citizens") == len(expected_versions)
    assert trino.scalar("select count(*) from clean.borgerportal.payments") == len(
        {p["payment_id"] for p in batch.payments}
    )
    pepper = pseudonym_pepper()
    sample = batch.citizens[0]["cpr"]
    assert (
        trino.scalar(
            "select count(*) from clean.borgerportal.citizens where citizen_id = ?", [pseudonymise(sample, pepper)]
        )
        > 0
    )
    amount, pay_date = trino.execute(
        "select amount, payment_date from clean.borgerportal.payments where payment_id = ?",
        [batch.payments[0]["payment_id"]],
    )[0]
    assert str(amount) == batch.payments[0]["amount"].replace(".", "").replace(",", ".")
    assert pay_date.strftime("%d-%m-%Y") == batch.payments[0]["payment_date"]

    # release 0.1 kept the CPR number in clean (fixed by migration V0001 in test_02); never beyond clean
    assert trino.scalar("select count(cpr_number) from clean.borgerportal.citizens") > 0
    assert scan_storage(fs, ["lakehouse-consolidated", "lakehouse-curated"], [sample]) == []
    assert scan_storage(fs, ["lakehouse-clean"], [sample]) != [], "sanity: the old shape stored the CPR in clean"
    assert scan_storage(fs, ["lakehouse-raw"], [sample]) != [], "sanity: the scanner must find the CPR in raw"
    assert not trino.table_exists("curated", "audit", "schema_migrations"), "release 0.1 had no migrations"

    # KEYSTORE
    ks = keystore(trino, "dim_citizen")
    assert_dense_keys(ks)
    held_back = {p["cpr"] for p in batch.payments} - {c["cpr"] for c in batch.citizens}
    inferred = {v["bk"] for v in ks.values() if v["type"] == "inferred"}
    assert inferred == {pseudonymise(c, pepper) for c in held_back} and inferred
    facts = fact_keys(trino)
    assert facts and all(ck > 0 and tk > 0 for ck, tk in facts.values()), "no fact row may fall back to -1"
    used = {ck for ck, _ in facts.values()}
    assert all(ks[k]["last_used"] is not None and ks[k]["last_used"] >= ks[k]["created"] for k in used)
    assert all(v["last_used"] is None for k, v in ks.items() if k not in used)
    assert_dense_keys(keystore(trino, "dim_case_type"))

    dbt("run-operation", "keystore_snapshot_baseline", "--args", "{dimensions: [dim_citizen, dim_case_type]}")
    history = {tuple(r) for r in trino.execute("select citizen_id, source_updated_at from clean.borgerportal.citizens")}
    state.update(ks1=ks, facts1=facts, inferred1=inferred, dim1=citizen_dim(trino), clean_citizens1=history)


# --- 2. release upgrade + changed load --------------------------------------------------------


def test_02_upgrade_migrates_without_a_wipe(trino, fs, s3, tmp_path):
    """N-5: the new release runs its migrations first (daily_refresh), then continues incrementally."""
    # outside dev a full refresh of an incremental model is refused before anything runs
    out = dbt("build", "--full-refresh", "--select", "citizens", env={"LAKEHOUSE_ENV": "prod"}, ok=False)
    assert "refused outside dev" in out
    assert trino.scalar("select count(*) from clean.borgerportal.citizens") == len(state["clean_citizens1"])

    result = run_job("daily_refresh", partition_key="2026-11-02")
    checks = [e for e in result.all_events if e.event_type_value == "ASSET_CHECK_EVALUATION"]
    assert len(checks) >= 40 and all(c.event_specific_data.passed for c in checks)
    steps = [e.step_key for e in result.all_events if e.event_type_value == "STEP_SUCCESS"]
    assert steps.index("curated__audit__schema_migrations") < steps.index("lakehouse_dbt_assets")

    applied = trino.execute("select version, name, applied_by from curated.audit.schema_migrations order by 1")
    assert applied[0] == ["0001", "clean_citizens_drop_cpr_number", "svc-dagster"]
    assert "cpr_number" not in clean_columns(trino, "citizens")
    # history of release 0.1 survives the upgrade: every citizen version of load 1 is still there
    history = {tuple(r) for r in trino.execute("select citizen_id, source_updated_at from clean.borgerportal.citizens")}
    assert state["clean_citizens1"] < history
    # the dropped column is physically gone: files rewritten, history expired; the replaced files'
    # noncurrent versions go with the maintenance job / bucket lifecycle (simulated here)
    from lakehouse.objectstore import purge_noncurrent

    purge_noncurrent(S3Settings.for_purge().filesystem(), "lakehouse-clean")
    sample = generator.simulate(SEED, 1, CITIZENS).citizens[0]["cpr"]
    assert scan_storage(fs, ["lakehouse-clean", "lakehouse-consolidated", "lakehouse-curated"], [sample]) == []
    cpr_columns = trino.execute(
        "select table_catalog from clean.information_schema.columns where column_name like '%cpr%' "
        "union all select table_catalog from consolidated.information_schema.columns where column_name like '%cpr%' "
        "union all select table_catalog from curated.information_schema.columns where column_name like '%cpr%'"
    )
    assert cpr_columns == []

    # running the migrations again changes nothing
    run_job("migrate")
    assert trino.execute("select version from curated.audit.schema_migrations order by 1") == [[v] for v, *_ in applied]
    # a migration on a table that does not exist (a new lake) is recorded without effect
    from governance.migrations.runner import record_table, run_migrations

    tmp = tmp_path / "migrations"
    shutil.copytree(ROOT / "transform" / "migrations", tmp)
    (tmp / "V9001__noop_on_missing_table.sql").write_text(
        "alter table if exists ${clean}.no_such_schema.no_such_table drop column if exists x;\n"
        "-- @purge-history clean.no_such_schema.no_such_table\n",
        encoding="utf-8",
    )
    assert run_migrations(directory=tmp).applied == ["V9001__noop_on_missing_table"]
    trino.execute(f"delete from {record_table()} where version = '9001'")


def test_03_changed_load_reuses_keys_and_builds_scd2(trino):
    ks1, ks2 = state["ks1"], keystore(trino, "dim_citizen")
    assert_dense_keys(ks2)

    # existing keys: never regenerated, creation timestamp untouched
    for sk, before in ks1.items():
        after = ks2[sk]
        assert after["bk"] == before["bk"] and after["created"] == before["created"]
        if before["type"] == "inferred":
            # master data arrived: the inferred member keeps its key and becomes a regular version
            assert after["type"] == "regular" and after["adopted"] is not None and after["vk"] != before["vk"]
        else:
            assert after["vk"] == before["vk"] and after["type"] == before["type"]
    new_keys = set(ks2) - set(ks1)
    assert new_keys and min(new_keys) > max(ks1), "new keys continue the sequence"

    # last-used semantics: only keys referenced by facts written in this run are re-stamped
    latest = trino.scalar("select max_by(_invocation_id, _transformed_at) from curated.borgerservice.fact_payment")
    used_now = {
        r[0]
        for r in trino.execute(
            "select distinct citizen_key from curated.borgerservice.fact_payment where _invocation_id = ?", [latest]
        )
    }
    assert used_now
    for sk, before in ks1.items():
        after = ks2[sk]
        if sk in used_now:
            assert after["last_used"] is not None
            assert before["last_used"] is None or after["last_used"] > before["last_used"]
        else:
            assert after["last_used"] == before["last_used"]
    assert all(ks2[sk]["last_used"] is not None for sk in used_now)

    # facts loaded in run 1 keep their keys
    facts2 = fact_keys(trino)
    assert all(facts2[p] == keys for p, keys in state["facts1"].items())
    assert len(facts2) > len(state["facts1"])

    # SCD2: changed citizens have several versions, intervals contiguous and non overlapping
    versions = trino.execute(
        "select person_id, valid_from, valid_to, is_current from curated.borgerservice.dim_citizen "
        "where not is_inferred and citizen_key > 0 order by person_id, valid_from"
    )
    by_person: dict[str, list] = {}
    for person, vf, vt, cur in versions:
        by_person.setdefault(person, []).append((vf, vt, cur))
    assert any(len(v) > 1 for v in by_person.values()), "the changed load must create SCD2 versions"
    for intervals in by_person.values():
        assert [c for _, _, c in intervals].count(True) == 1 and intervals[-1][2]
        for (_, vt, _), (vf_next, _, _) in itertools.pairwise(intervals):
            assert vt == vf_next
        assert all(vf < vt for vf, vt, _ in intervals)
    assert trino.scalar("select count(*) from curated.borgerservice.dim_citizen where is_current and is_deregistered")

    # every fact (incl. late arriving ones) points at the version valid on its payment date
    misplaced = trino.scalar(
        """select count(*) from curated.borgerservice.fact_payment f
           join clean.borgerportal.payments p on p.payment_id = f.payment_id
           join curated.borgerservice.dim_citizen d on d.citizen_key = f.citizen_key
           where not (cast(p.payment_date as timestamp(6) with time zone) >= d.valid_from
                      and cast(p.payment_date as timestamp(6) with time zone) < d.valid_to)"""
    )
    assert misplaced == 0
    late = trino.scalar("select count(*) from clean.borgerportal.payments where payment_date < cast(booked_at as date)")
    assert late > 0, "the changed load contains late arriving facts"
    # SCD1 dimension: new reference row got a key, existing ones unchanged
    types = keystore(trino, "dim_case_type")
    assert_dense_keys(types)
    assert {v["bk"] for v in types.values()} >= {"FLY", "BYG"}
    assert all(v["vk"] == "" for v in types.values() if v["type"] == "regular")
    # SCD2 history built by release 0.1 survives the upgrade: same versions, same keys
    dim2 = citizen_dim(trino)
    regular = {sk for sk, v in ks1.items() if v["type"] == "regular"}
    survived = {version: sk for version, sk in state["dim1"].items() if sk in regular}
    assert survived and all(dim2.get(version) == sk for version, sk in survived.items())
    state.update(ks2=ks2, facts2=facts2, dim2=dim2, types2=types)


# --- 3. rebuild --------------------------------------------------------------------------------


def test_04_full_rebuild_keeps_every_key(trino):
    trino.execute("drop table curated.borgerservice.dim_citizen")
    trino.execute("drop table curated.borgerservice.dim_case_type")
    out = dbt("build", "--full-refresh")  # clean, consolidated, curated rebuilt from the raw index
    assert "keystore_stable_since_baseline_dim_citizen" in out and "ERROR=0" in out

    ks3 = keystore(trino, "dim_citizen")
    strip = lambda ks: {k: (v["bk"], v["vk"], v["type"], v["created"]) for k, v in ks.items()}  # noqa: E731
    assert strip(ks3) == strip(state["ks2"]), "a rebuild must neither create nor change keys"
    assert citizen_dim(trino) == state["dim2"], "dimension rows get exactly the keys they had"
    assert fact_keys(trino) == state["facts2"], "facts resolve to the same keys after a full refresh"
    assert strip(keystore(trino, "dim_case_type")) == strip(state["types2"])


def test_05_rerun_without_new_data_is_idempotent(trino):
    from ingestion.framework.indexing import index_source

    assert index_source(SOURCE).rows == {}, "no committed load is pending"
    counts_before = trino.execute("select count(*) from raw.borgerportal.citizens")
    dbt("build")
    assert trino.execute("select count(*) from raw.borgerportal.citizens") == counts_before
    assert set(keystore(trino, "dim_citizen")) == set(state["ks2"])
    assert fact_keys(trino) == state["facts2"]


# --- 4. access control (OPA through Trino) -----------------------------------------------------


def _as(user: str, sql: str):
    return TrinoClient(user=user).execute(sql)


def test_06_access_control_and_masking():
    import trino.exceptions

    denied = trino.exceptions.TrinoUserError
    assert _as("anna-analyst", "show catalogs") == [["curated"], ["system"]]
    schemas = {r[0] for r in _as("anna-analyst", "show schemas from curated")}
    assert "borgerservice" in schemas and "keystore" not in schemas and "audit" not in schemas

    name, email, postal = _as(
        "anna-analyst",
        "select full_name, email, postal_code from curated.borgerservice.dim_citizen "
        "where citizen_key > 0 and full_name is not null limit 1",
    )[0]
    assert name == "*****" and email.startswith("*****@") and re.fullmatch(r"\d{4}", postal)
    clear = _as(
        "petra-pii",
        "select full_name, email from curated.borgerservice.dim_citizen "
        "where citizen_key > 0 and full_name is not null limit 1",
    )[0]
    assert clear[0] != "*****" and not clear[1].startswith("*****")

    for sql in (
        "select * from clean.borgerportal.citizens",
        "select * from raw.borgerportal.citizens",
        "select * from curated.keystore.dim_citizen",
        "delete from curated.borgerservice.fact_payment",
        "select * from system.runtime.queries",
    ):
        with pytest.raises(denied):
            _as("anna-analyst", sql)
    with pytest.raises(denied):
        _as("mallory", "select 1")

    # engineers work on all layers but see classified columns masked unless they are pii readers
    name, city = _as(
        "erik-engineer", "select first_name, city from clean.borgerportal.citizens where first_name is not null limit 1"
    )[0]
    assert name == "*****" and city
    assert _as("erik-engineer", "select payload from raw.borgerportal.citizens limit 1") == [[None]]
    payload = _as("svc-dagster", "select payload from raw.borgerportal.citizens limit 1")[0][0]
    assert re.search(r"\d{6}-\d{4}", payload), "the pipeline reads the raw record unmasked"

    # protected schemas: only the pipeline service account writes the keystore (M-1)
    for user in ("erik-engineer", "alice-admin"):
        with pytest.raises(denied):
            _as(user, "delete from curated.keystore.dim_citizen where surrogate_key = 1")
        with pytest.raises(denied):
            _as(user, "set session curated.expire_snapshots_min_retention = '0s'")

    # auditor: read only, masked, may inspect query history
    assert _as(
        "axel-auditor", "select first_name from clean.borgerportal.citizens where first_name is not null limit 1"
    ) == [["*****"]]
    assert _as("axel-auditor", "select count(*) from system.runtime.queries")[0][0] > 0
    with pytest.raises(denied):
        _as("axel-auditor", "delete from clean.borgerportal.citizens")

    # row filter: small cells of the data product are hidden from analysts
    total, small = _as(
        "petra-pii", "select count(*), count_if(is_small_cell) from curated.borgerservice.dp_payment_summary_v1"
    )[0]
    visible = _as("anna-analyst", "select count(*) from curated.borgerservice.dp_payment_summary_v1")[0][0]
    assert small > 0 and visible == total - small


# --- 5. right to erasure -----------------------------------------------------------------------


def test_07_erasure_removes_the_subject_everywhere(trino, fs, s3):
    key = trino.scalar(
        """select k.business_key from curated.keystore.dim_citizen k
           where k.business_key in (select citizen_id from clean.borgerportal.payments)
           group by k.business_key having count(*) > 1 limit 1"""
    )
    assert key, "need a citizen with history and payments"
    cpr = cpr_by_key()[key]
    subject_keys = [
        r[0]
        for r in trino.execute("select surrogate_key from curated.keystore.dim_citizen where business_key = ?", [key])
    ]
    assert len(subject_keys) > 1
    in_list = ", ".join(str(k) for k in subject_keys)
    fact_rows = trino.scalar(
        f"select count(*) from curated.borgerservice.fact_payment where citizen_key in ({in_list})"
    )
    max_key = max(keystore(trino, "dim_citizen"))

    before = scan_storage(fs, BUCKETS, [cpr, key])
    for bucket in BUCKETS:
        assert any(h.startswith(f"s3://{bucket}/") for h in before), f"sanity: subject must be present in {bucket}"
    # versioned buckets: deleted files (dropped dbt temp tables, the dimensions dropped in test_03,
    # expired snapshots) still hold the subject as noncurrent versions - the case C-1 is about
    stale = noncurrent_hits(fs, BUCKETS, [cpr, key])
    assert stale, "sanity: noncurrent object versions must contain the subject before the erasure"

    # dry run (N-8): counts only - it writes nothing, not even a queue row or a new snapshot
    def governance_snapshots():
        tables = trino.execute(
            "select table_name from raw.information_schema.tables where table_schema = 'governance' order by 1"
        )
        return {t: trino.scalar(f'select count(*) from raw.governance."{t}$snapshots"') for (t,) in tables}

    before_preview = governance_snapshots()
    preview = erase_subject(identifier=cpr, requested_by="dpo@kommune.example", reason="preview", dry_run=True)
    assert preview.rows["clean.borgerportal.citizens [delete]"] > 1 and preview.raw_files
    assert preview.rows["clean.borgerportal.citizens [delete]"] == trino.scalar(
        "select count(*) from clean.borgerportal.citizens where citizen_id = ?", [key]
    )
    assert governance_snapshots() == before_preview

    request_id = submit_request(identifier=cpr, requested_by="dpo@kommune.example", reason="integration test")
    result = run_job("erasure")  # GovernanceResource verifies the object store itself (ERASURE_VERIFY_STORAGE)
    events = json.dumps([str(e.event_specific_data) for e in result.all_events])
    assert cpr not in events and key not in events, "no identifier may leak into orchestrator events"

    # physically gone: no object version (current or noncurrent) contains the CPR number or the pseudonym
    assert scan_storage(fs, BUCKETS, [cpr, key]) == []
    assert all(not v.is_delete_marker or v.is_latest for b in BUCKETS for v in iter_versions(fs, b))
    assert all(v.is_latest for b in BUCKETS for v in iter_versions(fs, b)), "no noncurrent version survives"
    # H-1: no query issued by the erasure tooling carries the identifier or the pseudonym
    texts = erasure_query_texts(trino)
    assert len(texts) > 50, "sanity: the erasure queries are in the query history"
    assert not [t for t in texts if key in t or cpr in t or re.search(r"[0-9a-f]{64}", t)], "no key-like literal"
    assert any(request_id in t for t in texts), "statements reference the request id instead"
    for table, column in (
        ("clean.borgerportal.citizens", "citizen_id"),
        ("clean.borgerportal.cases", "citizen_id"),
        ("clean.borgerportal.payments", "citizen_id"),
        ("consolidated.party.person", "person_id"),
        ("curated.borgerservice.dim_citizen", "person_id"),
        ("curated.keystore.dim_citizen", "business_key"),
        ("curated.keystore.dim_citizen__baseline", "business_key"),
    ):
        assert trino.scalar(f"select count(*) from {table} where {column} = ?", [key]) == 0, table
    for entity in ("citizens", "cases", "payments"):
        assert (
            trino.scalar(f"select count(*) from raw.borgerportal.{entity} where _subject_keys like ?", [f"%{key}%"])
            == 0
        )
        assert trino.scalar(f"select count(*) from raw.borgerportal.{entity} where payload like ?", [f"%{cpr}%"]) == 0
    # time travel is gone as well: only the post-erasure snapshot remains
    assert trino.scalar('select count(*) from clean.borgerportal."citizens$snapshots"') == 1

    # keystore: integer keys survive as anonymous members and are never handed out again
    ks = keystore(trino, "dim_citizen")
    for sk in subject_keys:
        assert ks[sk]["type"] == "erased" and ks[sk]["bk"] == f"erased:{sk}"
    assert max(ks) == max_key
    # facts stay (bookkeeping) but only reference the anonymous member; indirect identifiers are nulled
    rows = trino.execute(
        f"select count(*), count(case_id) from curated.borgerservice.fact_payment where citizen_key in ({in_list})"
    )
    assert rows == [[fact_rows, 0]]
    erased_dim = trino.execute(
        f"select full_name, email, is_erased from curated.borgerservice.dim_citizen where citizen_key in ({in_list})"
    )
    assert len(erased_dim) == len(subject_keys) and all(r == [None, None, True] for r in erased_dim)

    # audit record without identifiers; request queue emptied
    audit = trino.execute(
        "select status, requested_by, executed_by, reason, subject_digest, details from curated.audit.erasure_log"
    )
    assert len(audit) == 1 and audit[0][:3] == ["completed", "dpo@kommune.example", "svc-dagster"]
    assert request_id in audit[0][3]
    assert cpr not in json.dumps(audit) and key not in json.dumps(audit)
    from governance.erasure.executor import audit_digest

    assert audit[0][4] == audit_digest(key, pseudonym_pepper()), "the keyed digest proves which subject was erased"
    assert trino.scalar(f"select count(*) from {queue_table_sql()}") == 0
    for user in ("erik-engineer", "alice-admin"):  # M-1: the evidence cannot be tampered with
        with pytest.raises(trino_errors.TrinoUserError):
            _as(user, "delete from curated.audit.erasure_log")
        with pytest.raises(trino_errors.TrinoUserError):
            _as(
                user,
                f"insert into {queue_table_sql()} (request_id, subject_key, identifier_type, requested_by, reason, "
                "requested_at) values ('x', 'y', 'z', 'u', 'v', current_timestamp)",
            )
    # a raw file that contained the subject is replaced by a tombstone, its other records stay indexed
    tombstones = [p for p in fs.find(s3.raw_bucket) if p.endswith(".erased.json")]
    assert tombstones and all(not fs.exists(p.removesuffix(".erased.json")) for p in tombstones)
    assert trino.scalar("select count(*) from raw.borgerportal.citizens") > 0

    # the lakehouse keeps working: rebuild is consistent and all tests still pass
    assert "ERROR=0" in dbt("build")
    after = keystore(trino, "dim_citizen")
    assert {sk: after[sk]["type"] for sk in subject_keys} == dict.fromkeys(subject_keys, "erased")
    state.update(erased_keys=subject_keys, max_key=max_key)


def test_08_next_load_after_erasure(trino):
    run_job("daily_refresh", partition_key="2026-11-03")
    ks = keystore(trino, "dim_citizen")
    assert_dense_keys(ks)
    for sk in state["erased_keys"]:
        assert ks[sk]["type"] == "erased", "erased keys are never reassigned"
    for sk, before in state["ks2"].items():
        if sk not in state["erased_keys"]:
            assert ks[sk]["bk"] == before["bk"] and ks[sk]["created"] == before["created"]


# --- 6. retention and maintenance --------------------------------------------------------------


def test_09_retention_and_maintenance_jobs(trino, fs, s3):
    from datetime import date

    from governance.retention.runner import run_retention

    facts = trino.scalar("select count(*) from curated.borgerservice.fact_payment")
    objects = len(fs.find(f"{s3.raw_bucket}/borgerportal/"))
    run_job("retention")
    assert trino.scalar("select count(*) from curated.borgerservice.fact_payment") == facts, "nothing is expired yet"
    assert len(fs.find(f"{s3.raw_bucket}/borgerportal/")) == objects

    future = run_retention(dry_run=True, today=date(2040, 1, 1))
    expired = {t["table"]: t["rows"] for t in future["tables"]}
    assert expired["curated.borgerservice.fact_payment"] == facts
    assert expired["raw.borgerportal.citizens"] > 0 and future["landing_objects"] > 0
    assert trino.scalar("select count(*) from curated.borgerservice.fact_payment") == facts, "dry run deletes nothing"

    # M-2: deregistered persons are queued for erasure once the grace period is over
    from governance.classification import manifest as cm
    from governance.config import load_retention, manifest_path
    from governance.retention.runner import plan_retention, queue_deregistered
    from ingestion.framework.spec import list_sources

    assert run_retention(dry_run=True)["deregistered_subjects"][0]["queued_for_erasure"] == 0, "grace period"
    (dereg,) = future["deregistered_subjects"]
    tombstoned = trino.scalar(
        "select count(*) from (select citizen_id, max_by(is_deleted, source_updated_at) d "
        "from clean.borgerportal.citizens group by 1) where d"
    )
    assert dereg["queued_for_erasure"] == tombstoned > 0
    plan = plan_retention(
        cm.tables_from_manifest(cm.load_manifest(manifest_path())), list_sources(), load_retention(), date(2040, 1, 1)
    )
    queue_deregistered(trino, plan)
    assert (
        trino.scalar(f"select count(*) from {queue_table_sql()} where requested_by = 'retention-policy'") == tombstoned
    )
    queue_deregistered(trino, plan)  # idempotent: already queued subjects are not queued twice
    assert trino.scalar(f"select count(*) from {queue_table_sql()}") == tombstoned
    assert not [t for t in erasure_query_texts(trino) if re.search(r"'[0-9a-f]{64}'", t)]
    trino.execute(f"delete from {queue_table_sql()} where requested_by = 'retention-policy'")

    result = run_job("iceberg_maintenance")
    ops = [e for e in result.all_events if e.event_type_value == "STEP_OUTPUT"]
    assert ops
    assert trino.scalar("select count(*) from curated.borgerservice.fact_payment") == facts
    assert "ERROR=0" in dbt("test")


# --- 7. concurrent key allocation ---------------------------------------------------------------


def test_10_concurrent_allocation_never_duplicates_keys(trino, tmp_path):
    """Three dbt processes allocate keys in the same keystore at the same moment (twice).

    Iceberg commits all of the colliding inserts; the macro must detect that, withdraw and retry.
    Whatever the timing, the keystore must end up without duplicates and without gaps.
    """
    succeeded = 0
    for round_no in (1, 2):
        procs = [
            subprocess.Popen(
                [
                    "uv",
                    "run",
                    "--no-sync",
                    "dbt",
                    "run-operation",
                    "keystore_selftest",
                    "--args",
                    f"{{tag: r{round_no}w{w}, count: 300}}",
                    "--target-path",
                    str(tmp_path / f"t{w}"),
                    "--log-path",
                    str(tmp_path / f"l{w}"),
                ],
                cwd=ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            for w in range(3)
        ]
        for proc in procs:
            out, _ = proc.communicate(timeout=300)
            # a writer may give up after all attempts collided (safe); anything else is a bug
            assert proc.returncode == 0 or "could not allocate keys without collision" in out, out[-2000:]
            succeeded += proc.returncode == 0
    ks = keystore(trino, "selftest_dim")
    assert_dense_keys(ks)
    assert len([k for k in ks if k > 0]) == 300 * succeeded and succeeded >= 1
    assert trino.scalar("select count(*) from curated.keystore.selftest_dim where surrogate_key = -1") == 1
