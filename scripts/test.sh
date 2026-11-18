#!/usr/bin/env bash
# Unit level tests that need no running platform: pytest, OPA policy tests, dbt parse,
# Dagster definitions validation.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "$ROOT"
need opa

log "dbt parse (manifest for orchestration + classification tests)"
"$ROOT/scripts/dbt.sh" parse >/dev/null
log "dbt full refresh guard selftest (no database needed)"
TRINO_HOST="${TRINO_HOST:-parse-only}" TRINO_USER="${TRINO_USER:-parse-only}" \
  "$ROOT/scripts/dbt.sh" run-operation full_refresh_guard_selftest | grep "selftest ok" >/dev/null \
  || die "full refresh guard selftest failed"

log "pytest"
uv run pytest -q "$@"

log "opa test"
opa test governance/policies

log "dagster definitions validate"
export TRINO_HOST="${TRINO_HOST:-unused}" TRINO_USER="${TRINO_USER:-unused}"
# instance-less validation: an ephemeral home under the (git-ignored) dist directory
mkdir -p "$ROOT/dist/dagster-home"
export DAGSTER_HOME="$ROOT/dist/dagster-home"
touch "$DAGSTER_HOME/dagster.yaml"
uv run dagster definitions validate -m orchestration.definitions 2>&1 | grep -E "Validation|rror" \
  || die "validation failed"
log "tests ok"
