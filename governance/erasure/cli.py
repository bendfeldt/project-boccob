"""``lakehouse-erase`` - right to erasure for one data subject.

The identifier is read from the environment or stdin by default so that it does not end up in
shell history or process listings:

    ERASURE_IDENTIFIER=... lakehouse-erase --requested-by dpo@kommune.example --reason "ticket 4711"
    echo "<cpr>" | lakehouse-erase --identifier-stdin --requested-by ... --reason ...
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from governance.erasure.executor import erase_subject


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse-erase", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--identifier-type", default="cpr")
    parser.add_argument("--identifier-stdin", action="store_true", help="read the identifier from stdin")
    parser.add_argument("--subject-key", help="pseudonymised key instead of an identifier")
    parser.add_argument("--requested-by", help="who approved the erasure (e.g. the DPO)")
    parser.add_argument("--reason", help="case / ticket reference")
    parser.add_argument("--dry-run", action="store_true", help="only report what would be erased")
    parser.add_argument("--submit", action="store_true",
                        help="queue the request for the scheduled Dagster erasure job instead of executing now")
    parser.add_argument("--process-pending", action="store_true", help="execute all queued requests")
    parser.add_argument("--verify-storage", action="store_true",
                        help="after purging, scan the object store for remains (slow)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if not args.process_pending and not (args.requested_by and args.reason):
        parser.error("--requested-by and --reason are required")
    if args.process_pending:
        from governance.erasure.executor import process_pending_requests

        reports = process_pending_requests(verify_storage=args.verify_storage)
        json.dump([r.as_dict() for r in reports], sys.stdout, indent=2, default=str)
        print()
        return 0

    identifier = None
    if not args.subject_key:
        identifier = sys.stdin.readline().strip() if args.identifier_stdin else os.environ.get("ERASURE_IDENTIFIER")
        if not identifier:
            parser.error("no identifier: set ERASURE_IDENTIFIER, use --identifier-stdin or --subject-key")
    if args.submit:
        from governance.erasure.executor import submit_request

        request_id = submit_request(identifier=identifier, identifier_type=args.identifier_type,
                                    subject_key=args.subject_key, requested_by=args.requested_by, reason=args.reason)
        print(json.dumps({"request_id": request_id, "status": "queued"}))
        return 0
    report = erase_subject(identifier=identifier, identifier_type=args.identifier_type, subject_key=args.subject_key,
                           requested_by=args.requested_by, reason=args.reason, dry_run=args.dry_run,
                           verify_storage=args.verify_storage)
    json.dump(report.as_dict(), sys.stdout, indent=2, default=str)
    print()
    return 0 if report.status in ("completed", "planned") else 1


if __name__ == "__main__":
    raise SystemExit(main())
