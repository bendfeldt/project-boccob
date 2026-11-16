#!/usr/bin/env bash
# Install pinned CLI tools into .tools/bin (no root needed). Downloads come from GitHub releases
# and are verified against the checksum files published with each release.
#   usage: scripts/install-tools.sh [opa] [oras] [syft] [trivy] [cosign]   (default: opa oras)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

OPA_VERSION="${OPA_VERSION:-1.22.4}"
ORAS_VERSION="${ORAS_VERSION:-1.3.6}"
SYFT_VERSION="${SYFT_VERSION:-1.57.1}"
TRIVY_VERSION="${TRIVY_VERSION:-0.76.2}"
COSIGN_VERSION="${COSIGN_VERSION:-3.1.4}"

BIN="$ROOT/.tools/bin"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$BIN"

fetch() { curl -fsSL --retry 3 -o "$2" "$1"; }

verify() { # file checksums-file  (checksum file lists "<sha256>  <name>")
  local file="$1" sums="$2" name want got
  name="$(basename "$file")"
  want="$(awk -v n="$name" '$2 == n || $2 == "*"n || $2 == "./"n {print $1}' "$sums" | head -n1)"
  [[ -n "$want" ]] || die "no checksum for $name"
  got="$(sha256sum "$file" | awk '{print $1}')"
  [[ "$want" == "$got" ]] || die "checksum mismatch for $name"
}

install_opa() {
  local base="https://github.com/open-policy-agent/opa/releases/download/v${OPA_VERSION}"
  fetch "$base/opa_linux_amd64_static" "$TMP/opa_linux_amd64_static"
  fetch "$base/opa_linux_amd64_static.sha256" "$TMP/opa.sha256"
  verify "$TMP/opa_linux_amd64_static" "$TMP/opa.sha256"
  install -m 0755 "$TMP/opa_linux_amd64_static" "$BIN/opa"
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
  install_tarball oras "$base/oras_${ORAS_VERSION}_linux_amd64.tar.gz" "$base/oras_${ORAS_VERSION}_checksums.txt" oras
}
install_syft() {
  local base="https://github.com/anchore/syft/releases/download/v${SYFT_VERSION}"
  install_tarball syft "$base/syft_${SYFT_VERSION}_linux_amd64.tar.gz" "$base/syft_${SYFT_VERSION}_checksums.txt" syft
}
install_trivy() {
  local base="https://github.com/aquasecurity/trivy/releases/download/v${TRIVY_VERSION}"
  install_tarball trivy "$base/trivy_${TRIVY_VERSION}_Linux-64bit.tar.gz" "$base/trivy_${TRIVY_VERSION}_checksums.txt" trivy
}
install_cosign() {
  local base="https://github.com/sigstore/cosign/releases/download/v${COSIGN_VERSION}"
  fetch "$base/cosign-linux-amd64" "$TMP/cosign-linux-amd64"
  fetch "$base/cosign_checksums.txt" "$TMP/cosign.sums"
  verify "$TMP/cosign-linux-amd64" "$TMP/cosign.sums"
  install -m 0755 "$TMP/cosign-linux-amd64" "$BIN/cosign"
}

tools=("$@"); [[ ${#tools[@]} -gt 0 ]] || tools=(opa oras)
for tool in "${tools[@]}"; do
  case "$tool" in
    opa|oras|syft|trivy|cosign) log "installing $tool"; "install_$tool" ;;
    *) die "unknown tool $tool" ;;
  esac
done
log "installed into $BIN: ${tools[*]}"
