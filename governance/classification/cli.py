"""``lakehouse-pii check|generate`` - enforce and export the column classification."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from governance.classification import manifest as m

REPO = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = Path(os.environ.get("DBT_PROJECT_DIR", REPO / "transform")) / "target" / "manifest.json"
DEFAULT_OUTPUT = REPO / "governance" / "policies" / "lakehouse" / "classification" / "data.json"


def _live_columns(tables: list[m.TableClass]) -> dict[str, set[str]]:
    from lakehouse.trino_client import TrinoClient, ident

    actual: dict[str, set[str]] = {}
    with TrinoClient() as trino:
        for catalog in sorted({t.catalog for t in tables if t.resource_type == "model"}):
            rows = trino.execute(
                f"select table_schema, table_name, column_name from {ident(catalog)}.information_schema.columns"
            )
            for schema, table, column in rows:
                actual.setdefault(f"{catalog}.{schema}.{table}", set()).add(column.lower())
    return actual


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse-pii", description=__doc__)
    parser.add_argument("command", choices=["check", "generate"])
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--live", action="store_true", help="check: also compare with the columns that exist in Trino")
    parser.add_argument(
        "--no-drift-check",
        action="store_true",
        help="check: do not compare the committed OPA data file with the manifest",
    )
    args = parser.parse_args(argv)

    tables = m.tables_from_manifest(m.load_manifest(args.manifest))
    problems = m.validate(tables)
    rendered = m.render(m.policy_document(tables))

    if args.command == "generate":
        if problems:
            print("\n".join(problems), file=sys.stderr)
            print(f"{len(problems)} classification problem(s); nothing written", file=sys.stderr)
            return 1
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
        print(f"wrote {args.output} ({len(tables)} tables)")
        return 0

    if args.live:
        problems += m.undocumented_columns(tables, _live_columns(tables))
    if not args.no_drift_check and (not args.output.is_file() or args.output.read_text(encoding="utf-8") != rendered):
        problems.append(f"{args.output} is out of date - run `make classification`")
    if problems:
        print("\n".join(problems), file=sys.stderr)
        print(f"{len(problems)} classification problem(s)", file=sys.stderr)
        return 1
    models = [t for t in tables if t.resource_type == "model"]
    print(f"classification ok: {sum(len(t.columns) for t in models)} columns in {len(models)} models")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
