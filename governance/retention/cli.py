"""``lakehouse-retention`` - apply the retention policy (governance/retention.yml + dbt meta)."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date

from governance.retention.runner import run_retention


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse-retention", description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report what would be deleted")
    parser.add_argument("--today", type=date.fromisoformat, help="evaluate the policy as of this date")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    json.dump(run_retention(dry_run=args.dry_run, today=args.today), sys.stdout, indent=2, default=str)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
