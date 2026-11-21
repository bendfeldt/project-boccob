#!/usr/bin/env bash
# dbt build with the release's schema migrations first (audit R3-4).
#   LAKEHOUSE_ENV=dev   pending migrations are applied (lakehouse-migrate, shared advisory lock)
#   otherwise           the build is refused while migrations are pending: apply them with the
#                       Dagster job `migrate` (writer pool), see docs/releases.md
# Extra arguments go to `dbt build`.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
cd "$ROOT"
"$ROOT/scripts/dbt.sh" parse >/dev/null   # the drift check reads the current manifest
if [[ "${LAKEHOUSE_ENV:-}" == "dev" ]]; then
  uv run lakehouse-migrate >/dev/null
else
  rc=0; uv run lakehouse-migrate --check >/dev/null || rc=$?
  [[ $rc -eq 0 ]] || die "schema migrations pending or check failed (exit $rc) - run the Dagster job 'migrate' first"
fi
exec "$ROOT/scripts/dbt.sh" build "$@"
