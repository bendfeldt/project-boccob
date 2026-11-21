#!/usr/bin/env bash
# Shared helpers for the solution scripts.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Local secrets and generated credentials live outside the checkout, never in the repo.
SOL_STATE_DIR="${SOL_STATE_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/boccob/solution}"
export HARNESS_STATE_DIR="${HARNESS_STATE_DIR:-$SOL_STATE_DIR/harness}"
export ROOT
export PATH="$ROOT/.tools/bin:$PATH"
export DBT_PROJECT_DIR="${DBT_PROJECT_DIR:-$ROOT/transform}"
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$ROOT/transform}"
export UV_NO_SYNC="${UV_NO_SYNC:-1}"

log() { printf '[%s] %s\n' "$(basename "$0")" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "$1 not found - run 'make tools' (or 'make setup' for python deps)"; }

# Platform of THIS machine for tool downloads: linux/amd64 (x64, x86_64), linux/arm64
# (aarch64) or darwin/arm64 (Apple Silicon). Anything else fails with a clear message.
host_platform() {
  local os arch m
  case "$(uname -s)" in
    Linux) os=linux ;;
    Darwin) os=darwin ;;
    *) die "operating system $(uname -s) is not supported (Linux or macOS)" ;;
  esac
  m="$(uname -m)"
  if [[ "$os" == darwin && "$m" == x86_64 && "$(sysctl -n sysctl.proc_translated 2>/dev/null || echo 0)" == 1 ]]; then
    die "this shell runs under Rosetta 2 (x86_64 emulation); use a native arm64 shell"
  fi
  case "$m" in
    x86_64|amd64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    i?86|x86) die "32-bit x86 ($m) is not supported: x64 (x86_64/amd64) or arm64 only" ;;
    *) die "CPU architecture $m is not supported: x64 (x86_64/amd64) or arm64 only" ;;
  esac
  [[ "$os/$arch" != darwin/amd64 ]] || die "Intel Macs are not supported by the pinned tool downloads (install opa/oras/syft/trivy/cosign with Homebrew)"
  printf '%s/%s' "$os" "$arch"
}

# Architecture of the machine that RUNS the containers (for macOS: the podman machine VM,
# not the Mac): amd64 or arm64.
container_arch() {
  local a
  a="$(podman info --format '{{.Host.Arch}}' 2>/dev/null)" || die "podman is not reachable (podman machine start?)"
  case "$a" in
    amd64|x86_64) echo amd64 ;;
    arm64|aarch64) echo arm64 ;;
    *) die "podman host architecture $a is not supported (amd64 or arm64)" ;;
  esac
}

# sha256 of a file (GNU coreutils or macOS shasum)
sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'
  else shasum -a 256 "$1" | awk '{print $1}'; fi
}

# Load local environment: the developer env file or the integration harness env, if present.
# Both live under $SOL_STATE_DIR (outside the checkout); secrets inside the repo are refused.
load_env() {
  local f
  for f in "$ROOT/.env" "$ROOT/.harness/env" "$ROOT/.dlt/secrets.toml"; do
    [[ -e "$f" ]] && die "secrets must not live in the repo: move ${f#"$ROOT"/} to $SOL_STATE_DIR/ (see README)"
  done
  # shellcheck disable=SC1091
  if [[ -f "$SOL_STATE_DIR/env" ]]; then set -a; source "$SOL_STATE_DIR/env"; set +a
  elif [[ -f "$HARNESS_STATE_DIR/env" ]]; then source "$HARNESS_STATE_DIR/env"; fi
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
