#!/usr/bin/env bash
# End-to-end test on the local podman harness (RustFS + Polaris + OPA + Trino):
# two loads, dimension rebuild, access control, erasure, retention, maintenance.
#   KEEP_HARNESS=1  leave the containers running afterwards (for debugging)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "$ROOT"
need podman
need opa

"$ROOT/scripts/dbt.sh" parse >/dev/null
uv run lakehouse-pii check --no-drift-check >/dev/null
cleanup() { [[ "${KEEP_HARNESS:-0}" == "1" ]] || "$ROOT/tests/integration/harness.sh" down; }
trap cleanup EXIT
"$ROOT/tests/integration/harness.sh" up
# shellcheck disable=SC1091
source "$ROOT/.harness/env"
mkdir -p "$ROOT/dist/dagster-home"
export DAGSTER_HOME="$ROOT/dist/dagster-home"
touch "$DAGSTER_HOME/dagster.yaml"
uv run pytest tests/integration -m integration -x -q -p no:cacheprovider "$@"
