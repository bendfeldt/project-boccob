"""``lakehouse-maintain`` - optimize / expire snapshots / remove orphan files."""

from __future__ import annotations

import argparse
import json
import logging
import sys

from governance.maintenance.runner import OPERATIONS, run_maintenance
from lakehouse.settings import LAYERS


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse-maintain", description=__doc__)
    parser.add_argument("--layer", action="append", choices=LAYERS, help="default: all layers")
    parser.add_argument("--schema", help="restrict to one schema")
    parser.add_argument("--operation", action="append", choices=OPERATIONS, help="default: all")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    report = run_maintenance(
        layers=tuple(args.layer or LAYERS), operations=tuple(args.operation or OPERATIONS), schema=args.schema
    )
    json.dump({"done": len(report.done), "failed": report.failed}, sys.stdout, indent=2)
    print()
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
