"""Versioned schema migrations for the lakehouse tables (audit N-5).

dbt handles additive changes of incremental models itself (``on_schema_change='append_new_columns'``).
Every other change of a table that holds history - dropping, renaming or retyping a column,
moving data - is a migration: a SQL file ``transform/migrations/V<nnnn>__<slug>.sql`` that brings an
existing lake to the shape the release's models expect, *before* the first dbt run of the release.
A full refresh is not an option outside dev (the raw layer keeps only 90 days, see guards.sql).

File format::

    -- free comment
    alter table if exists ${clean}.borgerportal.citizens drop column if exists cpr_number;
    -- @purge-history clean.borgerportal.citizens

* statements end with ``;`` at the end of a line; ``${raw}`` ``${clean}`` ``${consolidated}``
  ``${curated}`` become the (quoted) Trino catalog of the layer
* every statement must be idempotent and must tolerate a missing table (``IF EXISTS``): on a new lake
  the tables do not exist yet and the migration is recorded without effect; a migration that failed
  half way is simply run again
* ``-- @purge-history <layer>.<schema>.<table>`` rewrites every row of the table and expires its
  history afterwards (rewrite_all_rows + governance.iceberg.purge_history), e.g. so that a dropped
  personal-data column is physically gone and not only hidden; skipped when the table does not exist
* no key-like (64-hex) or CPR-shaped literal may appear: SQL text is archived WORM

Applied migrations are recorded in ``curated.audit.schema_migrations`` (protected schema: only the
service group writes). A recorded migration whose file has changed, or a recorded version that this
release does not know (the lake is newer than the code), stops the run.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

from governance import iceberg
from governance.config import catalog_of
from lakehouse.settings import LAYERS
from lakehouse.trino_client import TrinoClient, fqn, ident

log = logging.getLogger(__name__)

RECORD_SCHEMA = "audit"
RECORD_TABLE = "schema_migrations"
_FILE = re.compile(r"^V(\d{4})__([a-z0-9_]+)\.sql$")
_DIRECTIVE = re.compile(r"^--\s*@purge-history\s+(\w+)\.(\w+)\.(\w+)\s*$")
_PLACEHOLDER = re.compile(r"\$\{(\w+)\}")
_FORBIDDEN = (re.compile(r"[0-9a-fA-F]{64}"), re.compile(r"\b\d{6}-?\d{4}\b"))


class MigrationError(RuntimeError):
    pass


@dataclass(frozen=True)
class Migration:
    version: str  # "0001"
    name: str
    path: Path
    checksum: str
    statements: tuple[str, ...]
    purge_history: tuple[tuple[str, str, str], ...] = ()  # (layer, schema, table)

    @property
    def label(self) -> str:
        return f"V{self.version}__{self.name}"


@dataclass
class MigrationReport:
    applied: list[str] = field(default_factory=list)
    already_applied: list[str] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)  # dry run
    purged: list[str] = field(default_factory=list)
    drift: list[str] = field(default_factory=list)


def migrations_dir() -> Path:
    default = Path(os.environ.get("DBT_PROJECT_DIR", Path(__file__).resolve().parents[2] / "transform"))
    return Path(os.environ.get("MIGRATIONS_DIR", default / "migrations"))


def _catalogs() -> dict[str, str]:
    return {layer: ident(catalog_of(layer)) for layer in LAYERS}


def parse(path: Path) -> Migration:
    m = _FILE.match(path.name)
    if not m:
        raise MigrationError(f"{path.name}: migration files are named V<nnnn>__<lower_snake_slug>.sql")
    text = path.read_text(encoding="utf-8")
    for pattern in _FORBIDDEN:
        if pattern.search(text):
            raise MigrationError(f"{path.name}: contains a key-like or CPR-shaped literal (SQL text is archived)")
    purge: list[tuple[str, str, str]] = []
    body: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        directive = _DIRECTIVE.match(stripped)
        if directive:
            layer = directive.group(1)
            if layer not in LAYERS:
                raise MigrationError(f"{path.name}: @purge-history needs <layer>.<schema>.<table>, got {layer!r}")
            purge.append((layer, directive.group(2), directive.group(3)))
        elif stripped.startswith("--"):
            continue
        else:
            body.append(line)
    catalogs = _catalogs()

    def placeholder(match: re.Match) -> str:
        if match.group(1) not in catalogs:
            raise MigrationError(
                f"{path.name}: unknown placeholder ${{{match.group(1)}}} (use ${{raw}}, ${{clean}}, ...)"
            )
        return catalogs[match.group(1)]

    sql = _PLACEHOLDER.sub(placeholder, "\n".join(body))
    statements = tuple(s.strip() for s in re.split(r";\s*(?:\n|$)", sql) if s.strip())
    if not statements and not purge:
        raise MigrationError(f"{path.name}: no statement")
    return Migration(
        version=m.group(1),
        name=m.group(2),
        path=path,
        checksum=hashlib.sha256(text.encode()).hexdigest(),
        statements=statements,
        purge_history=tuple(purge),
    )


def discover(directory: Path | None = None) -> list[Migration]:
    directory = directory or migrations_dir()
    if not directory.is_dir():
        return []
    migrations = [parse(p) for p in sorted(directory.glob("*.sql"))]
    versions = [m.version for m in migrations]
    if len(set(versions)) != len(versions):
        raise MigrationError(f"duplicate migration version in {directory}: {sorted(versions)}")
    return sorted(migrations, key=lambda m: m.version)


def record_table() -> str:
    return fqn(catalog_of("curated"), RECORD_SCHEMA, RECORD_TABLE)


def ensure_record_table(trino: TrinoClient) -> str:
    trino.execute(f"create schema if not exists {ident(catalog_of('curated'))}.{ident(RECORD_SCHEMA)}")
    table = record_table()
    trino.execute(
        f"""create table if not exists {table} (
            version varchar, name varchar, checksum varchar, statements integer,
            applied_at timestamp(6) with time zone, applied_by varchar, duration_ms bigint)
            comment 'Applied lakehouse schema migrations (transform/migrations, docs/releases.md)'"""
    )
    return table


def applied_migrations(trino: TrinoClient) -> dict[str, tuple[str, str]]:
    """version -> (name, checksum) of every recorded migration (empty when none was ever run)."""
    table = ensure_record_table(trino)
    return {r[0]: (r[1], r[2]) for r in trino.execute(f"select version, name, checksum from {table}")}


def check(migrations: list[Migration], applied: dict[str, tuple[str, str]]) -> list[Migration]:
    """Validate the recorded history against the files; return the migrations still to apply."""
    known = {m.version: m for m in migrations}
    unknown = sorted(set(applied) - set(known))
    if unknown:
        raise MigrationError(
            f"the lake has migration(s) {unknown} that this release does not contain: it was migrated by a "
            "newer release. Deploy that release (or restore the lake) instead of rolling back."
        )
    for version, (name, checksum) in sorted(applied.items()):
        if known[version].checksum != checksum:
            raise MigrationError(
                f"V{version}__{name} was changed after it was applied; never edit an applied migration, add a new one"
            )
    return [m for m in migrations if m.version not in applied]


def rewrite_all_rows(trino: TrinoClient, ref: iceberg.TableRef) -> None:
    """Make sure no data file of the table survives the following purge.

    OPTIMIZE leaves a file alone when there is nothing to merge (e.g. a single file without delete
    files), and such a file would keep the values of a dropped column. A self-assignment of the
    first column marks every row deleted and rewrites it with the current schema; OPTIMIZE then
    has delete files to apply and replaces every old file. Costs one rewrite of the table.
    """
    first = trino.scalar(
        f"select column_name from {ident(ref.catalog)}.information_schema.columns "
        "where table_schema = ? and table_name = ? order by ordinal_position limit 1",
        [ref.schema, ref.table],
    )
    if first:
        trino.execute(f"update {ref.sql} set {ident(first)} = {ident(first)}")


def apply(trino: TrinoClient, migration: Migration) -> list[str]:
    started = time.monotonic()
    for statement in migration.statements:
        log.info("%s: %s", migration.label, statement.splitlines()[0][:120])
        trino.execute(statement)
    purged = []
    for layer, schema, table in migration.purge_history:
        ref = iceberg.TableRef(catalog_of(layer), schema, table)
        if trino.table_exists(ref.catalog, ref.schema, ref.table):
            rewrite_all_rows(trino, ref)
            iceberg.purge_history(trino, ref)
            purged.append(str(ref))
    result = trino.execute(
        f"insert into {record_table()} values (?, ?, ?, ?, current_timestamp, current_user, ?)",
        [
            migration.version,
            migration.name,
            migration.checksum,
            len(migration.statements),
            int((time.monotonic() - started) * 1000),
        ],
    )
    if not result or result[0][0] != 1:
        raise MigrationError(f"{migration.label}: the record row was not written")
    return purged


def schema_drift(trino: TrinoClient, tables) -> list[str]:
    """Differences between an incremental model's table and the model that a migration must fix.

    * a column in the lake that the model no longer has: ``append_new_columns`` would silently keep
      it (and its data)
    * a column whose live type differs from a ``data_type`` declared in the model yml

    Type changes of undeclared columns are caught when dbt runs the model (guards.sql:
    ``trino__process_schema_changes`` refuses ``new_target_types``), because only then is the
    model's new type known.
    """
    problems = []
    for t in tables:
        if t.resource_type != "model" or t.materialized != "incremental":
            continue
        rows = trino.execute(
            f"select column_name, data_type from {ident(t.catalog)}.information_schema.columns "
            "where table_schema = ? and table_name = ?",
            [t.schema, t.table],
        )
        documented = {c.name: c for c in t.columns}
        for row in rows:
            column, live_type = row[0].lower(), (row[1] if len(row) > 1 else None)
            if column not in documented:
                problems.append(f"{t.key}.{column}: in the lake but not in the model - ship a migration")
                continue
            declared = documented[column].data_type
            if declared and live_type and _norm_type(declared) != _norm_type(live_type):
                problems.append(
                    f"{t.key}.{column}: type {live_type} in the lake, {declared} in the model - ship a migration"
                )
    return problems


def _norm_type(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower()).replace(" (", "(").replace(", ", ",")


def run_migrations(
    *,
    dry_run: bool = False,
    directory: Path | None = None,
    trino: TrinoClient | None = None,
    check_drift: bool = True,
) -> MigrationReport:
    """Apply every pending migration in version order (Dagster asset/op ``migrate``, writer pool)."""
    migrations = discover(directory)
    trino = trino or TrinoClient(
        session_properties=iceberg.retention_session([catalog_of(layer) for layer in LAYERS], "0s"),
        source="lakehouse-migrate",
    )
    report = MigrationReport()
    applied = applied_migrations(trino)
    pending = check(migrations, applied)
    report.already_applied = [m.label for m in migrations if m.version in applied]
    if dry_run:
        report.pending = [m.label for m in pending]
        return report
    for migration in pending:
        report.purged += apply(trino, migration)
        report.applied.append(migration.label)
        log.info("applied %s", migration.label)
    if check_drift:
        from governance.classification import manifest as cm
        from governance.config import manifest_path

        report.drift = schema_drift(trino, cm.tables_from_manifest(cm.load_manifest(manifest_path())))
        if report.drift:
            raise MigrationError("schema drift after migrations: " + "; ".join(report.drift))
    return report
