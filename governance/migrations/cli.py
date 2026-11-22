"""``lakehouse-migrate`` - apply the pending schema migrations (transform/migrations).

    lakehouse-migrate --dry-run   # list applied and pending migrations, change nothing
    lakehouse-migrate --check     # exit 3 when migrations are pending, change nothing
    lakehouse-migrate             # dev only: apply pending migrations in version order, check drift

Outside dev (LAKEHOUSE_ENV != dev) migrations are applied only by the Dagster job ``migrate`` (and
the first step of transform, erasure and retention), under the ``lakehouse_writer`` pool: the CLI
is not serialised with the other writers, and ``@purge-history`` uses a zero retention window.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from governance.migrations.runner import run_migrations
from lakehouse.settings import is_dev

EXIT_PENDING = 3


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lakehouse-migrate", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="only list applied / pending migrations")
    mode.add_argument("--check", action="store_true", help=f"exit {EXIT_PENDING} if migrations are pending")
    parser.add_argument("--no-drift-check", action="store_true", help="skip the live column drift check")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if args.dry_run or args.check:
        report = run_migrations(dry_run=True)
        json.dump(report.__dict__, sys.stdout, indent=2)
        print()
        if args.check and report.pending:
            print(f"pending migrations: {report.pending} - run the Dagster job `migrate`", file=sys.stderr)
            return EXIT_PENDING
        return 0
    if not is_dev():
        parser.error("outside dev, migrations are applied by the Dagster job `migrate` (writer pool)")
    report = run_migrations(check_drift=not args.no_drift_check)
    json.dump(report.__dict__, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
