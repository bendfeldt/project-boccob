#!/usr/bin/env bash
# Install pinned CLI tools into .tools/bin (no root needed). Downloads come from GitHub releases
# and are verified against the checksum files published with each release. The build matches
# this machine: linux/amd64 (x64), linux/arm64 or darwin/arm64 (Apple Silicon); see host_platform.
#   usage: scripts/install-tools.sh [opa] [oras] [syft] [trivy]   (default: opa oras)
# Signing (cosign) is a platform function: the platform holds the key.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

OPA_VERSION="${OPA_VERSION:-1.22.4}"
ORAS_VERSION="${ORAS_VERSION:-1.3.6}"
SYFT_VERSION="${SYFT_VERSION:-1.57.1}"
TRIVY_VERSION="${TRIVY_VERSION:-0.76.2}"

BIN="$ROOT/.tools/bin"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$BIN"
PLATFORM="$(host_platform)"
OS="${PLATFORM%/*}" ARCH="${PLATFORM#*/}"
log "platform: $PLATFORM"

fetch() { curl -fsSL --retry 3 -o "$2" "$1"; }

verify() { # file checksums-file  (checksum file lists "<sha256>  <name>")
  local file="$1" sums="$2" name want got
  name="$(basename "$file")"
  want="$(awk -v n="$name" '$2 == n || $2 == "*"n || $2 == "./"n {print $1}' "$sums" | head -n1)"
  [[ -n "$want" ]] || die "no checksum for $name"
  got="$(sha256_file "$file")"
  [[ "$want" == "$got" ]] || die "checksum mismatch for $name"
}

install_opa() {
  local base="https://github.com/open-policy-agent/opa/releases/download/v${OPA_VERSION}"
  local f="opa_${OS}_${ARCH}_static"
  fetch "$base/$f" "$TMP/$f"
  fetch "$base/$f.sha256" "$TMP/opa.sha256"
  verify "$TMP/$f" "$TMP/opa.sha256"
  install -m 0755 "$TMP/$f" "$BIN/opa"
}

install_tarball() { # name url checksums-url binary-in-archive
  local name="$1" url="$2" sums="$3" member="$4" archive
  archive="$TMP/$(basename "$url")"
  fetch "$url" "$archive"
  fetch "$sums" "$TMP/$name.sums"
  verify "$archive" "$TMP/$name.sums"
  mkdir -p "$TMP/$name"
  tar -xzf "$archive" -C "$TMP/$name"
  install -m 0755 "$TMP/$name/$member" "$BIN/$name"
}

install_oras() {
  local base="https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}"
  install_tarball oras "$base/oras_${ORAS_VERSION}_${OS}_${ARCH}.tar.gz" "$base/oras_${ORAS_VERSION}_checksums.txt" oras
}
install_syft() {
  local base="https://github.com/anchore/syft/releases/download/v${SYFT_VERSION}"
  install_tarball syft "$base/syft_${SYFT_VERSION}_${OS}_${ARCH}.tar.gz" "$base/syft_${SYFT_VERSION}_checksums.txt" syft
}
install_trivy() {
  local base="https://github.com/aquasecurity/trivy/releases/download/v${TRIVY_VERSION}"
  local name
  case "$PLATFORM" in
    linux/amd64) name=Linux-64bit ;; linux/arm64) name=Linux-ARM64 ;; darwin/arm64) name=macOS-ARM64 ;;
  esac
  install_tarball trivy "$base/trivy_${TRIVY_VERSION}_${name}.tar.gz" "$base/trivy_${TRIVY_VERSION}_checksums.txt" trivy
}
tools=("$@"); [[ ${#tools[@]} -gt 0 ]] || tools=(opa oras)
for tool in "${tools[@]}"; do
  case "$tool" in
    opa|oras|syft|trivy) log "installing $tool"; "install_$tool" ;;
    *) die "unknown tool $tool" ;;
  esac
done
log "installed into $BIN: ${tools[*]}"
