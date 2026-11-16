"""Right to erasure (GDPR art. 17) - planning.

The plan is derived from metadata, never hand-maintained:
* ingestion source specs  -> which raw index tables hold person identifiers
* dbt manifest meta.gdpr  -> what to do with each model:
    delete     rows of the subject are deleted (``subject_column`` = pseudonymised key)
    keystore   dimension: rows are replaced by an anonymous "erased" member; the keystore entry
               keeps its integer key (never reused) but loses the business key
    anonymise  fact: rows stay (e.g. bookkeeping duty) but only point at the erased member;
               ``anonymise_columns`` are set to NULL
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from governance.classification.manifest import TableClass
from governance.config import catalog_of
from ingestion.framework.spec import SourceSpec
from lakehouse.trino_client import fqn, ident

_KEY = re.compile(r"^[0-9a-f]{64}$")
KEYSTORE_SCHEMA = "keystore"


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
    subject_key: str
    raw_file_queries: list[tuple[str, str]] = field(default_factory=list)  # (table, sql listing _raw_file)
    steps: list[Step] = field(default_factory=list)

    @property
    def tables(self) -> list[str]:
        seen: dict[str, None] = {}
        for step in self.steps:
            seen.setdefault(step.table)
        return list(seen)


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


def build_plan(subject_key: str, tables: list[TableClass], sources: list[SourceSpec]) -> ErasurePlan:
    key = validate_subject_key(subject_key)
    plan = ErasurePlan(subject_key=key)
    lit = f"'{key}'"

    # 1. raw index: every entity that declares person identifiers
    for source in sources:
        for entity in source.entities:
            if not entity.identifiers:
                continue
            table = fqn(catalog_of("raw"), source.name, entity.name)
            name = f"{catalog_of('raw')}.{source.name}.{entity.name}"
            match = " or ".join(
                f"json_extract_scalar(_subject_keys, '$.{ident(n)[1:-1]}') = {lit}" for n in sorted(entity.identifiers)
            )
            plan.raw_file_queries.append((name, f"select distinct _raw_file from {table} where {match}"))
            plan.steps.append(Step(
                table=name, action="raw_index",
                statements=(f"delete from {table} where {match}",),
                count_sql=f"select count(*) from {table} where {match}",
                verify_sql=f"select count(*) from {table} where {match}",
            ))

    models = [t for t in tables if t.resource_type == "model"]
    order = {"clean": 0, "consolidated": 1, "curated": 2}

    # 2. facts first (they need the keystore entries to find the subject's surrogate keys)
    for t in sorted(models, key=lambda t: order.get(t.layer or "", 9)):
        if t.gdpr.get("erasure") != "anonymise":
            continue
        dim = _by_name(models, t.gdpr["subject_dimension"])
        key_col = ident(t.gdpr["subject_key_column"])
        keys = f"select surrogate_key from {keystore_table(dim.name)} where business_key = {lit}"
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
        plan.steps.append(Step(
            table=t.key, action="anonymise", statements=statements,
            count_sql=f"select count(*) from {target} where {key_col} in ({keys})",
            verify_sql=verify,
        ))

    # 3. plain deletes, clean -> consolidated -> curated
    for t in sorted(models, key=lambda t: order.get(t.layer or "", 9)):
        if t.gdpr.get("erasure") != "delete":
            continue
        target = fqn(t.catalog, t.schema, t.table)
        where = f"{ident(t.gdpr['subject_column'])} = {lit}"
        plan.steps.append(Step(
            table=t.key, action="delete",
            statements=(f"delete from {target} where {where}",),
            count_sql=f"select count(*) from {target} where {where}",
            verify_sql=f"select count(*) from {target} where {where}",
        ))

    # 4. dimensions + keystore last
    for t in models:
        if t.gdpr.get("erasure") != "keystore":
            continue
        s = _dimension_settings(t)
        dim = fqn(t.catalog, t.schema, t.table)
        ks = keystore_table(t.name)
        sk, bk = ident(s["surrogate_key"]), ident(s["business_key"])
        scd2_cols = ", valid_from, valid_to, is_current" if s["scd_type"] == 2 else ""
        scd2_vals = (", timestamp '1900-01-01 00:00:00.000000 UTC', timestamp '9999-12-31 00:00:00.000000 UTC', true"
                     if s["scd_type"] == 2 else "")
        statements = (
            # anonymous placeholders keep facts joinable; they carry no attribute and no business key
            f"insert into {dim} ({sk}, {bk}, is_inferred, is_erased{scd2_cols}) "
            f"select surrogate_key, 'erased:' || cast(surrogate_key as varchar), false, true{scd2_vals} "
            f"from {ks} where business_key = {lit}",
            f"delete from {dim} where {bk} = {lit}",
            f"update {ks} set business_key = 'erased:' || cast(surrogate_key as varchar), "
            f"version_key = 'erased', member_type = 'erased' where business_key = {lit}",
        )
        plan.steps.append(Step(
            table=t.key, action="keystore", statements=statements,
            count_sql=f"select count(*) from {dim} where {bk} = {lit}",
            verify_sql=(f"select (select count(*) from {dim} where {bk} = {lit}) "
                        f"+ (select count(*) from {ks} where business_key = {lit})"),
        ))
        plan.steps.append(Step(
            table=f"{catalog_of('curated')}.{KEYSTORE_SCHEMA}.{t.name}", action="keystore", statements=(),
            count_sql=f"select count(*) from {ks} where business_key = {lit}",
            verify_sql=f"select count(*) from {ks} where business_key = {lit}",
        ))
    return plan
