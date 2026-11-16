#!/usr/bin/env bash
# Shared helpers for the solution scripts.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export ROOT
export PATH="$ROOT/.tools/bin:$PATH"
export DBT_PROJECT_DIR="${DBT_PROJECT_DIR:-$ROOT/transform}"
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$ROOT/transform}"
export UV_NO_SYNC="${UV_NO_SYNC:-1}"

log() { printf '[%s] %s\n' "$(basename "$0")" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "$1 not found - run 'make tools' (or 'make setup' for python deps)"; }

# Load local environment: .env (developer) or the integration harness env, if present.
load_env() {
  if [[ -f "$ROOT/.env" ]]; then set -a; source "$ROOT/.env"; set +a
  elif [[ -f "$ROOT/.harness/env" ]]; then source "$ROOT/.harness/env"; fi
}

# Version of the artifacts: explicit VERSION, else git describe.
artifact_version() {
  if [[ -n "${VERSION:-}" ]]; then echo "$VERSION"; return; fi
  local v; v="$(git -C "$ROOT" describe --tags --always --dirty 2>/dev/null || echo dev)"
  echo "0.1.0-${v}"
}
