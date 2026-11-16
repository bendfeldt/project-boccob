#!/usr/bin/env bash
# Run dbt against the configured Trino (environment from .env / harness / the pod).
#   usage: scripts/dbt.sh build | test | run --select ... | parse
# `parse` needs no connection: placeholders are used when TRINO_HOST / TRINO_USER are unset.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
for arg in "$@"; do
  if [[ "$arg" == "parse" ]]; then
    export TRINO_HOST="${TRINO_HOST:-parse-only}" TRINO_USER="${TRINO_USER:-parse-only}"
  fi
done
cd "$ROOT"
exec uv run dbt "$@"
