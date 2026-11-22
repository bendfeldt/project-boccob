"""Command line entry point: ``lakehouse-ingest land|index|run|list``."""

from __future__ import annotations

import argparse
import json
import logging
import sys

from ingestion.framework.spec import list_sources, load_source


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse-ingest", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list", help="list configured sources")
    for name, text in (
        ("land", "stage 1: land native files"),
        ("index", "stage 2: index landed loads"),
        ("run", "land then index"),
    ):
        p = sub.add_parser(name, help=text)
        p.add_argument("source")
        if name != "index":
            p.add_argument("--logical-date", help="business date of the delivery (YYYY-MM-DD)")
        if name == "index":
            p.add_argument("--load-id", help="index exactly this load")
            p.add_argument("--reindex", action="store_true", help="replace the rows of --load-id (dev only)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if args.command == "list":
        for spec in list_sources():
            print(f"{spec.name}: {', '.join(f'{e.name}({e.format})' for e in spec.entities)}")
        return 0

    from ingestion.framework.indexing import index_source
    from ingestion.framework.landing import land_source

    spec = load_source(args.source)
    out = {}
    if args.command in ("land", "run"):
        out["land"] = land_source(spec, logical_date=args.logical_date).summary()
    if args.command == "index":
        out["index"] = index_source(spec, load_id=args.load_id, reindex=args.reindex).summary()
    if args.command == "run":
        out["index"] = index_source(spec).summary()
    json.dump(out, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
