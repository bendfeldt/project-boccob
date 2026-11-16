#!/usr/bin/env bash
# Static checks: python (ruff), shell (shellcheck), Rego (opa fmt/check), dbt project (parse),
# PII classification completeness.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "$ROOT"
need opa

log "ruff"
uv run ruff check .
uv run ruff format --check .

if command -v shellcheck >/dev/null 2>&1; then
  log "shellcheck"
  shellcheck -e SC1091 scripts/*.sh tests/integration/*.sh
else
  log "shellcheck not installed - skipped"
fi

log "opa fmt / check"
unformatted="$(opa fmt --list governance/policies)"
[[ -z "$unformatted" ]] || die "rego files need 'opa fmt -w': $unformatted"
opa check --strict governance/policies

log "dbt parse"
"$ROOT/scripts/dbt.sh" --warn-error parse >/dev/null

log "PII classification"
uv run lakehouse-pii check
log "lint ok"
