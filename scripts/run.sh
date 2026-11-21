#!/usr/bin/env bash
# Run a solution command with the local environment loaded.
#   scripts/run.sh lakehouse-ingest run borgerportal
#   scripts/run.sh lakehouse-erase --dry-run --requested-by ... --reason ...
#   scripts/run.sh lakehouse-retention --dry-run
#   scripts/run.sh lakehouse-maintain --layer curated
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
# dagster dev would otherwise put its instance (run storage, logs) in the checkout
export DAGSTER_HOME="${DAGSTER_HOME:-$SOL_STATE_DIR/dagster-home}"
mkdir -p "$DAGSTER_HOME"; touch "$DAGSTER_HOME/dagster.yaml"
[[ $# -gt 0 ]] || die "usage: scripts/run.sh <command> [args...]"
cd "$ROOT"
exec uv run "$@"
