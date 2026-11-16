#!/usr/bin/env bash
# PII classification: `generate` writes the OPA data file from the dbt manifest,
# `check` enforces that every clean+ column is classified and that the data file is current.
#   usage: scripts/classification.sh generate | check [--live]
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
"$ROOT/scripts/dbt.sh" parse >/dev/null
cd "$ROOT"
exec uv run lakehouse-pii "$@"
