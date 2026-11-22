"""PII classification derived from the dbt manifest.

Every column of every model in the clean, consolidated and curated layers must declare
``config.meta.pii`` (none | indirect | direct | sensitive). From the same metadata the data
document for the OPA policies (column masks, row filters) and the erasure/retention plans are
generated, so there is exactly one place where a column is classified: the dbt schema file.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

PII_LEVELS = ("none", "indirect", "direct", "sensitive")
MASK_STRATEGIES = ("null", "redact", "email", "hash")
GOVERNED_LAYERS = ("clean", "consolidated", "curated")
ERASURE_ACTIONS = ("delete", "anonymise", "keystore", "none")


class ClassificationError(ValueError):
    pass


@dataclass(frozen=True)
class ColumnClass:
    name: str
    pii: str | None
    mask: str | None = None
    cpr: bool = False
    pseudonymised: bool = False
    data_type: str | None = None  # only when declared in the model yml

    def as_policy(self) -> dict[str, Any]:
        doc: dict[str, Any] = {"pii": self.pii}
        if self.mask:
            doc["mask"] = self.mask
        if self.cpr:
            doc["cpr"] = True
        if self.pseudonymised:
            doc["pseudonymised"] = True
        return doc


@dataclass(frozen=True)
class TableClass:
    unique_id: str
    resource_type: str
    name: str
    catalog: str
    schema: str
    table: str
    layer: str | None
    materialized: str | None
    columns: tuple[ColumnClass, ...]
    gdpr: dict[str, Any] = field(default_factory=dict)
    row_filter: dict[str, Any] | None = None
    config: dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


def _meta(node: dict[str, Any]) -> dict[str, Any]:
    """dbt >= 1.10 (required by dbt_project.yml) keeps meta under config."""
    return dict((node.get("config") or {}).get("meta") or {})


def _column(name: str, col: dict[str, Any]) -> ColumnClass:
    meta = _meta(col)
    mask = meta.get("mask")
    return ColumnClass(
        name=name.lower(),
        pii=meta.get("pii"),
        mask=str(mask) if mask is not None else None,
        cpr=bool(meta.get("cpr", False)),
        pseudonymised=bool(meta.get("pseudonymised", False)),
        data_type=(str(col["data_type"]).strip().lower() or None) if col.get("data_type") else None,
    )


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ClassificationError(f"dbt manifest not found: {path} (run `make dbt-parse`)")
    return json.loads(path.read_text(encoding="utf-8"))


def tables_from_manifest(manifest: dict[str, Any]) -> list[TableClass]:
    tables: list[TableClass] = []
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        meta = _meta(node)
        tables.append(
            TableClass(
                unique_id=node["unique_id"],
                resource_type="model",
                name=node["name"],
                catalog=node["database"],
                schema=node["schema"],
                table=node.get("alias") or node["name"],
                layer=meta.get("layer"),
                materialized=(node.get("config") or {}).get("materialized"),
                columns=tuple(_column(n, c) for n, c in (node.get("columns") or {}).items()),
                gdpr=dict(meta.get("gdpr") or {}),
                row_filter=meta.get("row_filter"),
                config=dict(node.get("config") or {}),
            )
        )
    for node in manifest.get("sources", {}).values():
        meta = _meta(node)
        source_meta = node.get("source_meta") or {}
        tables.append(
            TableClass(
                unique_id=node["unique_id"],
                resource_type="source",
                name=node["name"],
                catalog=node["database"],
                schema=node["schema"],
                table=node.get("identifier") or node["name"],
                layer=meta.get("layer") or source_meta.get("layer"),
                materialized=None,
                columns=tuple(_column(n, c) for n, c in (node.get("columns") or {}).items()),
                gdpr=dict(meta.get("gdpr") or {}),
            )
        )
    return sorted(tables, key=lambda t: t.key)


def validate(tables: list[TableClass]) -> list[str]:
    """Return human readable violations (empty list = compliant)."""
    problems: list[str] = []
    for t in tables:
        if t.resource_type != "model":
            continue
        if t.layer not in GOVERNED_LAYERS:
            problems.append(f"{t.name}: model has no meta.layer in {GOVERNED_LAYERS}")
            continue
        if not t.columns:
            problems.append(f"{t.key}: no columns documented")
        for c in t.columns:
            if c.pii is None:
                problems.append(f"{t.key}.{c.name}: missing meta.pii classification")
            elif c.pii not in PII_LEVELS:
                problems.append(f"{t.key}.{c.name}: invalid pii level {c.pii!r}")
            if c.mask is not None and c.mask not in MASK_STRATEGIES:
                problems.append(f"{t.key}.{c.name}: invalid mask strategy {c.mask!r}")
            if c.cpr and c.pii not in ("direct", "sensitive"):
                problems.append(f"{t.key}.{c.name}: CPR columns must be classified direct or sensitive")
        has_personal = any(c.pii in ("indirect", "direct", "sensitive") for c in t.columns)
        action = t.gdpr.get("erasure")
        if action is not None and action not in ERASURE_ACTIONS:
            problems.append(f"{t.key}: invalid gdpr.erasure {action!r}")
        subject_bound = any(c.pseudonymised or c.cpr for c in t.columns)
        if has_personal and subject_bound and not action:
            problems.append(f"{t.key}: holds person-bound data but declares no meta.gdpr.erasure")
        if action in ("delete", "keystore") and t.gdpr.get("subject_column") not in {c.name for c in t.columns}:
            problems.append(f"{t.key}: gdpr.subject_column must name a documented column")
        if action == "anonymise" and not (t.gdpr.get("subject_dimension") and t.gdpr.get("subject_key_column")):
            problems.append(f"{t.key}: gdpr.erasure=anonymise needs subject_dimension and subject_key_column")
        # kept anonymised until its own retention expires when a deregistered person is erased
        timed = bool(t.gdpr.get("retention_days") and t.gdpr.get("retention_column"))
        if action == "delete" and timed and "_anonymised_at" not in {c.name for c in t.columns}:
            problems.append(f"{t.key}: delete table with its own retention needs an _anonymised_at column")
        dereg = t.gdpr.get("deregistration")
        if dereg is not None:
            names = {c.name for c in t.columns}
            if not isinstance(dereg, dict) or not {"flag_column", "since_column", "grace_days"} <= set(dereg):
                problems.append(f"{t.key}: gdpr.deregistration needs flag_column, since_column, grace_days")
            elif dereg["flag_column"] not in names or dereg["since_column"] not in names:
                problems.append(f"{t.key}: gdpr.deregistration columns must be documented columns")
            elif t.gdpr.get("subject_column") not in names:
                problems.append(f"{t.key}: gdpr.deregistration needs gdpr.subject_column")
        if t.row_filter is not None and not t.row_filter.get("expression"):
            problems.append(f"{t.key}: meta.row_filter needs an expression")
    return problems


def undocumented_columns(tables: list[TableClass], actual: dict[str, set[str]]) -> list[str]:
    """Compare documented columns with the columns that really exist (``{table key: {columns}}``)."""
    problems = []
    for t in tables:
        if t.resource_type != "model" or t.key not in actual:
            continue
        documented = {c.name for c in t.columns}
        for missing in sorted(actual[t.key] - documented):
            problems.append(f"{t.key}.{missing}: column exists in the lakehouse but is not classified")
        for stale in sorted(documented - actual[t.key]):
            problems.append(f"{t.key}.{stale}: classified column does not exist in the lakehouse")
    return problems


def policy_document(tables: list[TableClass]) -> dict[str, Any]:
    """The JSON document served to OPA as ``data.lakehouse.classification``."""
    doc: dict[str, Any] = {"generated_from": "dbt manifest (governance.classification)", "tables": {}}
    for t in tables:
        entry: dict[str, Any] = {
            "layer": t.layer,
            "columns": {c.name: c.as_policy() for c in t.columns if c.pii is not None},
        }
        if t.row_filter:
            entry["row_filter"] = {
                "expression": t.row_filter["expression"],
                "exempt_groups": sorted(t.row_filter.get("exempt_groups", [])),
            }
        doc["tables"][t.key] = entry
    return doc


def render(doc: dict[str, Any]) -> str:
    return json.dumps(doc, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
