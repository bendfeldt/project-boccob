"""``lakehouse-migrate`` - apply the pending schema migrations (transform/migrations).

    lakehouse-migrate --dry-run   # list applied and pending migrations, change nothing
    lakehouse-migrate --check     # exit 3 when migrations are pending, change nothing
    lakehouse-migrate             # apply pending migrations in version order, then check drift

Normally the Dagster job ``migrate`` (and the first step of daily_refresh, transform, erasure and
retention) applies them under the ``lakehouse_writer`` pool. The CLI takes the same advisory lock
(``curated.audit.schema_migration_lock``), so it never runs concurrently with a Dagster migration,
but it is *not* serialised with other writers (dbt, erasure): ``@purge-history`` uses a zero
retention window. Outside dev (LAKEHOUSE_ENV != dev) applying therefore needs ``--outside-dagster``
as the operator's confirmation that schedules are paused and no run is active.
"""

from __future__ import annotations

import argparse
import getpass
import json
import logging
import os
import sys

from governance.migrations.runner import run_migrations

EXIT_PENDING = 3


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lakehouse-migrate", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="only list applied / pending migrations")
    mode.add_argument("--check", action="store_true", help=f"exit {EXIT_PENDING} if migrations are pending")
    parser.add_argument("--no-drift-check", action="store_true", help="skip the live column drift check")
    parser.add_argument(
        "--outside-dagster",
        action="store_true",
        help="confirm that no Dagster writer run is active (required to apply outside LAKEHOUSE_ENV=dev)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    dev = os.environ.get("LAKEHOUSE_ENV", "").strip().lower() == "dev"
    if args.dry_run or args.check:
        report = run_migrations(dry_run=True)
        json.dump(report.__dict__, sys.stdout, indent=2)
        print()
        if args.check and report.pending:
            print(f"pending migrations: {report.pending} - run the Dagster job `migrate`", file=sys.stderr)
            return EXIT_PENDING
        return 0
    if not dev and not args.outside_dagster:
        parser.error(
            "outside dev, apply migrations with the Dagster job `migrate`; or pause the schedules, make "
            "sure no run is active and pass --outside-dagster"
        )
    report = run_migrations(check_drift=not args.no_drift_check, holder=f"cli {getpass.getuser()}")
    json.dump(report.__dict__, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
