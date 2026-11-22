"""``lakehouse-erase`` - right to erasure for one data subject.

The identifier is read from the environment or stdin so that it does not end up in shell
history or process listings:

    ERASURE_IDENTIFIER=... lakehouse-erase --requested-by dpo@kommune.example --reason "ticket 4711"
    echo "<cpr>" | lakehouse-erase --identifier-stdin --requested-by ... --reason ...
    lakehouse-erase --dry-run [--scope deregistration]     # tables and actions, no identifier

Outside dev (LAKEHOUSE_ENV != dev) a request is only queued: the Dagster job ``erasure`` executes
it under the writer pool. Executing here could race a running dbt MERGE, whose uncommitted files
the zero-retention purge would delete. In dev it is executed at once unless ``--submit``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from lakehouse.settings import is_dev


def _dump(obj) -> None:
    json.dump(obj, sys.stdout, indent=2, default=str)
    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lakehouse-erase", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--identifier-type", default="cpr")
    parser.add_argument("--identifier-stdin", action="store_true", help="read the identifier from stdin")
    parser.add_argument("--subject-key", help="pseudonymised key instead of an identifier")
    parser.add_argument("--requested-by", help="who approved the erasure (e.g. the DPO)")
    parser.add_argument("--reason", help="case / ticket reference")
    parser.add_argument("--dry-run", action="store_true", help="list the tables and actions; needs no identifier")
    parser.add_argument("--scope", default="full", choices=["full", "deregistration"], help="for --dry-run")
    parser.add_argument("--submit", action="store_true", help="only queue (always the case outside dev)")
    parser.add_argument("--process-pending", action="store_true", help="execute all queued requests (dev only)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    from governance.erasure import executor

    if args.dry_run:
        _dump(executor.preview(scope=args.scope).as_dict())
        return 0
    if args.process_pending:
        if not is_dev():
            parser.error("outside dev the Dagster job `erasure` executes the queue (writer pool)")
        try:
            reports = executor.process_pending_requests()
        except executor.ErasureBatchError as exc:
            reports = exc.reports
        _dump([r.as_dict() for r in reports])
        return 0 if all(r.status == "completed" for r in reports) else 1
    if not (args.requested_by and args.reason):
        parser.error("--requested-by and --reason are required")
    identifier = None
    if not args.subject_key:
        identifier = sys.stdin.readline().strip() if args.identifier_stdin else os.environ.get("ERASURE_IDENTIFIER")
        if not identifier:
            parser.error("no identifier: set ERASURE_IDENTIFIER, use --identifier-stdin or --subject-key")
    request = {
        "identifier": identifier,
        "identifier_type": args.identifier_type,
        "subject_key": args.subject_key,
        "requested_by": args.requested_by,
        "reason": args.reason,
    }
    if args.submit or not is_dev():
        print(json.dumps({"request_id": executor.submit_request(**request), "status": "queued"}))
        return 0
    try:
        report = executor.erase_subject(**request)
    except executor.ErasureBatchError as exc:
        report = exc.reports[0]
    _dump(report.as_dict())
    return 0 if report.status == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
