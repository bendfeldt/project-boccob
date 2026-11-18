"""``lakehouse-migrate`` - apply the pending schema migrations (transform/migrations).

    lakehouse-migrate --dry-run   # list applied and pending migrations, change nothing
    lakehouse-migrate             # apply pending migrations in version order, then check drift

Normally run by the Dagster job ``migrate`` (and before every dbt build in ``daily_refresh`` /
``transform``), which holds the ``lakehouse_writer`` pool. Run by hand only when no other writer is
active: ``@purge-history`` uses a zero retention window.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from governance.migrations.runner import run_migrations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lakehouse-migrate", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--dry-run", action="store_true", help="only list applied / pending migrations")
    parser.add_argument("--no-drift-check", action="store_true", help="skip the live column drift check")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    report = run_migrations(dry_run=args.dry_run, check_drift=not args.no_drift_check)
    json.dump(report.__dict__, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
