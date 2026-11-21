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

# Registry trust and proxy bypass for one publishing script, from the environment only
# (no writes to /etc, nothing persists after the script):
#   REGISTRY_CA_FILE  PEM of the CA that signed the registry certificate (e.g. the platform PKI)
#   REGISTRY_HOST     host[:port] to bypass the proxy for (default: host part of REGISTRY)
# Sets REGISTRY_PODMAN_ARGS (--cert-dir <temp dir with ca.crt>) and REGISTRY_ORAS_ARGS (--ca-file),
# exports SSL_CERT_FILE (system trust + the CA, for cosign) and appends the host to NO_PROXY/no_proxy.
# shellcheck disable=SC2034 # read by the scripts that source lib.sh
REGISTRY_PODMAN_ARGS=() REGISTRY_ORAS_ARGS=()
# shellcheck disable=SC2034
registry_trust() {
  local host="${REGISTRY_HOST:-${REGISTRY%%/*}}"
  if [[ -n "${REGISTRY_CA_FILE:-}" ]]; then
    [[ -r "$REGISTRY_CA_FILE" ]] || die "REGISTRY_CA_FILE=$REGISTRY_CA_FILE is not readable"
    grep -q "BEGIN CERTIFICATE" "$REGISTRY_CA_FILE" || die "REGISTRY_CA_FILE is not a PEM certificate"
    REGISTRY_TRUST_DIR="$(mktemp -d)"
    # shellcheck disable=SC2064 # expand now: the path is fixed for this script
    trap "rm -rf -- '$REGISTRY_TRUST_DIR'" EXIT
    mkdir -p "$REGISTRY_TRUST_DIR/certs"
    cp "$REGISTRY_CA_FILE" "$REGISTRY_TRUST_DIR/certs/ca.crt"
    local system=""  # keep the trust already in effect (e.g. an inspecting proxy's CA), add the registry CA
    for f in "${SSL_CERT_FILE:-}" /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt; do
      [[ -r "$f" ]] && { system="$f"; break; }
    done
    { [[ -n "$system" ]] && cat "$system"; cat "$REGISTRY_CA_FILE"; } > "$REGISTRY_TRUST_DIR/bundle.pem"
    REGISTRY_PODMAN_ARGS=(--cert-dir "$REGISTRY_TRUST_DIR/certs")
    REGISTRY_ORAS_ARGS=(--ca-file "$REGISTRY_CA_FILE")
    export SSL_CERT_FILE="$REGISTRY_TRUST_DIR/bundle.pem"
  fi
  if [[ -n "$host" ]]; then
    local entries="$host"
    [[ "$host" == *:* ]] && entries="$host,${host%%:*}"
    export NO_PROXY="${NO_PROXY:+$NO_PROXY,}$entries" no_proxy="${no_proxy:+$no_proxy,}$entries"
  fi
}
