#!/usr/bin/env bash
# Build the Dagster code location image with podman, then generate an SBOM and a vulnerability
# report when syft / trivy are available (make tools-supply-chain).
#
#   REGISTRY            registry host[:port] used in the tag (default: localhost)
#   REGISTRY_NAMESPACE  repository prefix (default: boccob)
#   VERSION             artifact version (default: 0.1.0-<git describe>)
#   PYTHON_IMAGE / UV_IMAGE   override the pinned base images (mirrored refs for air-gap)
#   UV_DEFAULT_INDEX    python package index (internal mirror for air-gap)
#   BUILD_NETWORK       podman --network value for the build (e.g. host)
#   BUILD_CA_BUNDLE     CA bundle passed to the build as a secret (internal CA / inspecting proxy)
#   BUILD_PROXY         https proxy for the build (passed as build-arg HTTPS_PROXY)
#   SCAN_SEVERITY       trivy severities that fail the build when SCAN_STRICT=1 (default HIGH,CRITICAL)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need podman

VERSION="$(artifact_version)"
REPO="${REGISTRY:-localhost}/${REGISTRY_NAMESPACE:-boccob}/dagster-code-location"
REF="${REPO}:${VERSION}"
OUT="$ROOT/dist/image"
mkdir -p "$OUT"

args=(--file "$ROOT/images/dagster-code-location/Containerfile" --tag "$REF"
      --build-arg "VERSION=${VERSION}"
      --build-arg "REVISION=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo unknown)")
[[ -n "${PYTHON_IMAGE:-}" ]] && args+=(--build-arg "PYTHON_IMAGE=${PYTHON_IMAGE}")
[[ -n "${UV_IMAGE:-}" ]] && args+=(--build-arg "UV_IMAGE=${UV_IMAGE}")
[[ -n "${UV_DEFAULT_INDEX:-}" ]] && args+=(--build-arg "UV_DEFAULT_INDEX=${UV_DEFAULT_INDEX}")
[[ -n "${BUILD_NETWORK:-}" ]] && args+=(--network "${BUILD_NETWORK}")
[[ -n "${BUILD_CA_BUNDLE:-}" ]] && args+=(--secret "id=ca_bundle,src=${BUILD_CA_BUNDLE}")
[[ -n "${BUILD_PROXY:-}" ]] && args+=(--build-arg "HTTPS_PROXY=${BUILD_PROXY}" --build-arg "https_proxy=${BUILD_PROXY}")

log "building $REF"
podman build "${args[@]}" "$ROOT"
echo "$REF" > "$OUT/image.tag"

log "smoke test: the code location must load inside the image (non-root, read-only rootfs)"
podman run --rm --read-only --tmpfs /tmp --cap-drop ALL --security-opt no-new-privileges --network none \
  -e TRINO_HOST=unused -e TRINO_USER=unused "$REF" \
  dagster definitions validate -m orchestration.definitions >/dev/null
test "$(podman run --rm --network none "$REF" id -u)" = "10001" || die "image does not run as uid 10001"

if command -v syft >/dev/null 2>&1 || command -v trivy >/dev/null 2>&1; then
  podman save --format docker-archive --output "$OUT/image.tar" "$REF" >/dev/null
  if command -v syft >/dev/null 2>&1; then
    log "generating SBOM (SPDX + CycloneDX)"
    syft "docker-archive:$OUT/image.tar" --quiet \
      --output "spdx-json=$OUT/sbom.spdx.json" --output "cyclonedx-json=$OUT/sbom.cdx.json"
  else
    log "syft not installed - no SBOM (make tools-supply-chain)"
  fi
  if command -v trivy >/dev/null 2>&1; then
    log "scanning for vulnerabilities"
    strict=0; [[ "${SCAN_STRICT:-0}" == "1" ]] && strict=1
    trivy image --input "$OUT/image.tar" --quiet --scanners vuln \
      --severity "${SCAN_SEVERITY:-HIGH,CRITICAL}" --ignore-unfixed \
      --format json --output "$OUT/trivy.json" --exit-code "$strict" \
      || { [[ "$strict" == "1" ]] && die "vulnerabilities found (see $OUT/trivy.json)"; log "trivy scan could not complete"; }
  else
    log "trivy not installed - no vulnerability scan (make tools-supply-chain)"
  fi
  rm -f "$OUT/image.tar"
else
  log "syft/trivy not installed - skipping SBOM and scan (make tools-supply-chain)"
fi
log "built $REF"
echo "$REF"
