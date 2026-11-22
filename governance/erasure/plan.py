"""Right to erasure (GDPR art. 17) - planning.

The plan is derived from metadata, never hand-maintained:
* ingestion source specs  -> which raw index tables hold person identifiers
* dbt manifest meta.gdpr  -> what to do with each model:
    delete     rows of the subject are deleted (``subject_column`` = pseudonymised key)
    keystore   dimension: rows are replaced by an anonymous "erased" member; the keystore entry
               keeps its integer key (never reused) but loses the business key
    anonymise  fact: rows stay (e.g. bookkeeping duty) but only point at the erased member;
               ``anonymise_columns`` are set to NULL

Scope of a request (``raw.governance.erasure_requests.scope``):
    full            art. 17 request of the data subject: the plan above
    deregistration  queued by the retention policy one year after deregistration (controller's
                    decision, docs/gdpr.md): personal identifiers go at once, but a ``delete`` table
                    with its own time rule (``retention_days`` + ``retention_column``, e.g. 5-year
                    bookkeeping) keeps the rows whose retention has not expired - anonymised: the
                    subject column and ``anonymise_columns`` become NULL, ``_anonymised_at`` is set.
                    Expired rows are deleted. The normal retention job deletes the rest later.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, timedelta

from governance.classification.manifest import TableClass
from governance.config import catalog_of
from ingestion.framework.spec import SourceSpec
from lakehouse.trino_client import fqn, ident

_KEY = re.compile(r"^[0-9a-f]{64}$")
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
KEYSTORE_SCHEMA = "keystore"
SCOPES = ("full", "deregistration")
ANONYMISED_MARKER = "_anonymised_at"


class ErasurePlanError(ValueError):
    pass


@dataclass(frozen=True)
class Step:
    table: str  # catalog.schema.table that is modified
    action: str  # delete | keystore | anonymise | raw_index
    statements: tuple[str, ...]
    count_sql: str  # rows currently held for the subject
    verify_sql: str  # must return 0 after execution


@dataclass
class ErasurePlan:
    request_id: str
    raw_file_queries: list[tuple[str, str]] = field(default_factory=list)  # (table, sql listing _raw_file)
    steps: list[Step] = field(default_factory=list)

    @property
    def tables(self) -> list[str]:
        seen: dict[str, None] = {}
        for step in self.steps:
            seen.setdefault(step.table)
        return list(seen)


def validate_request_id(request_id: str) -> str:
    if not _UUID.match(request_id or ""):
        raise ErasurePlanError("request id must be a lower-case UUID")
    return request_id


def validate_subject_key(subject_key: str) -> str:
    if not _KEY.match(subject_key):
        raise ErasurePlanError("subject key must be a 64 character lower-case hex pseudonym")
    return subject_key


def _by_name(tables: list[TableClass], name: str) -> TableClass:
    matches = [t for t in tables if t.resource_type == "model" and t.name == name]
    if len(matches) != 1:
        raise ErasurePlanError(f"dimension model {name!r} not found in the dbt manifest")
    return matches[0]


def _dimension_settings(dim: TableClass) -> dict:
    cfg = dim.config
    if cfg.get("materialized") != "dimension":
        raise ErasurePlanError(f"{dim.name} is not a keystore dimension")
    return {
        "business_key": cfg["business_key"],
        "surrogate_key": cfg.get("surrogate_key") or dim.name.replace("dim_", "") + "_key",
        "scd_type": int(cfg.get("scd_type", 2)),
    }


def keystore_table(dimension: str) -> str:
    return fqn(catalog_of("curated"), KEYSTORE_SCHEMA, dimension)


def subject_query(request_id: str) -> str:
    """SQL that yields the subject key of a queued request.

    The key itself never appears in SQL text (query text is archived in the WORM audit trail);
    statements carry only the opaque request id.
    """
    from governance.erasure.queue import queue_table_sql

    return f"select subject_key from {queue_table_sql()} where request_id = '{validate_request_id(request_id)}'"


def retained_until_expiry(t: TableClass) -> bool:
    """A ``delete`` table with its own time rule: kept anonymised on a deregistration erasure."""
    return t.gdpr.get("erasure") == "delete" and bool(t.gdpr.get("retention_days") and t.gdpr.get("retention_column"))


def _retain_anonymised_step(t: TableClass, subject: str, today: date) -> Step:
    names = {c.name for c in t.columns}
    if ANONYMISED_MARKER not in names:
        raise ErasurePlanError(f"{t.key}: kept anonymised on deregistration but has no {ANONYMISED_MARKER} column")
    target = fqn(t.catalog, t.schema, t.table)
    subject_col = ident(t.gdpr["subject_column"])
    where = f"{subject_col} in ({subject})"
    cutoff = today - timedelta(days=int(t.gdpr["retention_days"]))
    expired = f"cast({ident(t.gdpr['retention_column'])} as date) < date '{cutoff.isoformat()}'"
    nulls = [subject_col] + [ident(c) for c in t.gdpr.get("anonymise_columns", []) if c in names]
    sets = ", ".join(f"{c} = null" for c in nulls)
    return Step(
        table=t.key,
        action="retain_anonymised",
        statements=(
            f"delete from {target} where {where} and {expired}",  # retention already over: gone now
            f"update {target} set {sets}, {ident(ANONYMISED_MARKER)} = current_timestamp where {where}",
        ),
        count_sql=f"select count(*) from {target} where {where}",
        verify_sql=f"select count(*) from {target} where {where}",
    )


def build_plan(
    request_id: str,
    tables: list[TableClass],
    sources: list[SourceSpec],
    *,
    scope: str = "full",
    today: date | None = None,
) -> ErasurePlan:
    if scope not in SCOPES:
        raise ErasurePlanError(f"scope must be one of {SCOPES}, got {scope!r}")
    plan = ErasurePlan(request_id=validate_request_id(request_id))
    subject = subject_query(request_id)
    today = today or date.today()

    # 1. raw index: every entity that declares person identifiers
    for source in sources:
        for entity in source.entities:
            if not entity.identifiers:
                continue
            table = fqn(catalog_of("raw"), source.name, entity.name)
            name = f"{catalog_of('raw')}.{source.name}.{entity.name}"
            match = " or ".join(
                f"json_extract_scalar(_subject_keys, '$.{ident(n)[1:-1]}') in ({subject})"
                for n in sorted(entity.identifiers)
            )
            plan.raw_file_queries.append((name, f"select distinct _raw_file from {table} where {match}"))
            plan.steps.append(
                Step(
                    table=name,
                    action="raw_index",
                    statements=(f"delete from {table} where {match}",),
                    count_sql=f"select count(*) from {table} where {match}",
                    verify_sql=f"select count(*) from {table} where {match}",
                )
            )

    models = [t for t in tables if t.resource_type == "model"]
    order = {"clean": 0, "consolidated": 1, "curated": 2}

    # 2. facts first (they need the keystore entries to find the subject's surrogate keys)
    for t in sorted(models, key=lambda t: order.get(t.layer or "", 9)):
        if t.gdpr.get("erasure") != "anonymise":
            continue
        dim = _by_name(models, t.gdpr["subject_dimension"])
        key_col = ident(t.gdpr["subject_key_column"])
        keys = f"select surrogate_key from {keystore_table(dim.name)} where business_key in ({subject})"
        target = fqn(t.catalog, t.schema, t.table)
        cols = [ident(c) for c in t.gdpr.get("anonymise_columns", [])]
        statements = ()
        verify = "select 0"
        if cols:
            sets = ", ".join(f"{c} = null" for c in cols)
            not_null = " or ".join(f"{c} is not null" for c in cols)
            statements = (f"update {target} set {sets} where {key_col} in ({keys})",)
            # after the keystore step the business key is gone: verify through erased members
            erased = f"select surrogate_key from {keystore_table(dim.name)} where member_type = 'erased'"
            verify = f"select count(*) from {target} where {key_col} in ({erased}) and ({not_null})"
        plan.steps.append(
            Step(
                table=t.key,
                action="anonymise",
                statements=statements,
                count_sql=f"select count(*) from {target} where {key_col} in ({keys})",
                verify_sql=verify,
            )
        )

    # 3. plain deletes, clean -> consolidated -> curated
    for t in sorted(models, key=lambda t: order.get(t.layer or "", 9)):
        if t.gdpr.get("erasure") != "delete":
            continue
        if scope == "deregistration" and retained_until_expiry(t):
            plan.steps.append(_retain_anonymised_step(t, subject, today))
            continue
        target = fqn(t.catalog, t.schema, t.table)
        where = f"{ident(t.gdpr['subject_column'])} in ({subject})"
        plan.steps.append(
            Step(
                table=t.key,
                action="delete",
                statements=(f"delete from {target} where {where}",),
                count_sql=f"select count(*) from {target} where {where}",
                verify_sql=f"select count(*) from {target} where {where}",
            )
        )

    # 4. dimensions + keystore last
    for t in models:
        if t.gdpr.get("erasure") != "keystore":
            continue
        s = _dimension_settings(t)
        dim = fqn(t.catalog, t.schema, t.table)
        ks = keystore_table(t.name)
        sk, bk = ident(s["surrogate_key"]), ident(s["business_key"])
        scd2_cols = ", valid_from, valid_to, is_current" if s["scd_type"] == 2 else ""
        scd2_vals = (
            ", timestamp '1900-01-01 00:00:00.000000 UTC', timestamp '9999-12-31 00:00:00.000000 UTC', true"
            if s["scd_type"] == 2
            else ""
        )
        statements = (
            # anonymous placeholders keep facts joinable; they carry no attribute and no business key
            f"insert into {dim} ({sk}, {bk}, is_inferred, is_erased{scd2_cols}) "
            f"select surrogate_key, 'erased:' || cast(surrogate_key as varchar), false, true{scd2_vals} "
            f"from {ks} where business_key in ({subject})",
            f"delete from {dim} where {bk} in ({subject})",
            f"update {ks} set business_key = 'erased:' || cast(surrogate_key as varchar), "
            f"version_key = 'erased', member_type = 'erased' where business_key in ({subject})",
        )
        plan.steps.append(
            Step(
                table=t.key,
                action="keystore",
                statements=statements,
                count_sql=f"select count(*) from {dim} where {bk} in ({subject})",
                verify_sql=(
                    f"select (select count(*) from {dim} where {bk} in ({subject})) "
                    f"+ (select count(*) from {ks} where business_key in ({subject}))"
                ),
            )
        )
        plan.steps.append(
            Step(
                table=f"{catalog_of('curated')}.{KEYSTORE_SCHEMA}.{t.name}",
                action="keystore",
                statements=(),
                count_sql=f"select count(*) from {ks} where business_key in ({subject})",
                verify_sql=f"select count(*) from {ks} where business_key in ({subject})",
            )
        )
    return plan
