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
#   CONTAINERFILE / SMOKE_TEST=0 / EMULATION_CHECK=0   other Containerfile / no smoke test / no
#                       qemu check (tests of this script only: a Containerfile without RUN steps builds
#                       for a foreign platform without emulation)
#   PLATFORMS           target platform(s), comma separated (default: linux/<arch of the podman host>,
#                       i.e. the podman machine VM on macOS). One platform: a plain image, as before.
#                       Several (linux/amd64,linux/arm64): one image per platform (<ref>-<arch>) plus a
#                       podman manifest list <ref>; push-image.sh pushes the index and records the
#                       INDEX digest. A foreign architecture needs qemu-user-static (binfmt_misc).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need podman

VERSION="$(artifact_version)"
REPO="${REGISTRY:-localhost}/${REGISTRY_NAMESPACE:-boccob}/dagster-code-location"
REF="${REPO}:${VERSION}"
OUT="$ROOT/dist/image"
mkdir -p "$OUT"

# platforms: linux/amd64 and/or linux/arm64
native="linux/$(container_arch)"
IFS=',' read -ra platforms <<<"${PLATFORMS:-$native}"
for p in "${platforms[@]}"; do
  case "$p" in linux/amd64|linux/arm64) ;; *) die "PLATFORMS: $p is not supported (linux/amd64, linux/arm64)" ;; esac
  if [[ "$p" != "$native" && "$(uname -s)" == Linux && "${EMULATION_CHECK:-1}" == "1" ]]; then
    want="qemu-aarch64"; [[ "$p" == linux/amd64 ]] && want="qemu-x86_64"
    [[ -e "/proc/sys/fs/binfmt_misc/$want" ]] \
      || die "building $p on $native needs user-mode emulation: install qemu-user-static (binfmt_misc entry $want missing)"
  fi
done

args=(--file "${CONTAINERFILE:-$ROOT/images/dagster-code-location/Containerfile}"
      --build-arg "VERSION=${VERSION}"
      --build-arg "REVISION=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo unknown)")
[[ -n "${PYTHON_IMAGE:-}" ]] && args+=(--build-arg "PYTHON_IMAGE=${PYTHON_IMAGE}")
[[ -n "${UV_IMAGE:-}" ]] && args+=(--build-arg "UV_IMAGE=${UV_IMAGE}")
[[ -n "${UV_DEFAULT_INDEX:-}" ]] && args+=(--build-arg "UV_DEFAULT_INDEX=${UV_DEFAULT_INDEX}")
[[ -n "${BUILD_NETWORK:-}" ]] && args+=(--network "${BUILD_NETWORK}")
[[ -n "${BUILD_CA_BUNDLE:-}" ]] && args+=(--secret "id=ca_bundle,src=${BUILD_CA_BUNDLE}")
[[ -n "${BUILD_PROXY:-}" ]] && args+=(--build-arg "HTTPS_PROXY=${BUILD_PROXY}" --build-arg "https_proxy=${BUILD_PROXY}")

smoke_test() { # image ref, platform   (SMOKE_TEST=0 skips it: only for build-script tests)
  [[ "${SMOKE_TEST:-1}" == "1" ]] || { log "smoke test skipped (SMOKE_TEST=0)"; return 0; }
  log "smoke test ($2): the code location must load inside the image (non-root, read-only rootfs)"
  podman run --rm --platform "$2" --read-only --tmpfs /tmp --cap-drop ALL --security-opt no-new-privileges --network none \
    -e TRINO_HOST=unused -e TRINO_USER=unused "$1" \
    dagster definitions validate -m orchestration.definitions >/dev/null
  test "$(podman run --rm --platform "$2" --network none "$1" id -u)" = "10001" || die "image does not run as uid 10001"
}

# local image names per platform (SBOM/scan below; removed by the IaC release after the push)
declare -a built=()
if [[ ${#platforms[@]} -eq 1 ]]; then
  log "building $REF (${platforms[0]})"
  # a manifest list of an earlier multi-platform build with the same name would shadow the image
  if podman manifest exists "$REF" 2>/dev/null; then podman manifest rm "$REF" >/dev/null; fi
  podman build "${args[@]}" --platform "${platforms[0]}" --tag "$REF" "$ROOT"
  built=("$REF")
  smoke_test "$REF" "${platforms[0]}"
else
  log "building $REF for ${platforms[*]} (manifest list)"
  podman manifest exists "$REF" && podman manifest rm "$REF" >/dev/null
  podman image exists "$REF" && podman rmi "$REF" >/dev/null
  podman manifest create "$REF" >/dev/null
  for p in "${platforms[@]}"; do
    pref="${REF}-${p#linux/}"
    podman build "${args[@]}" --platform "$p" --tag "$pref" "$ROOT"
    podman manifest add "$REF" "containers-storage:$pref" >/dev/null
    built+=("$pref")
    # foreign platforms run under emulation only when asked for (slow)
    if [[ "$p" == "$native" || "${SMOKE_ALL:-0}" == "1" ]]; then smoke_test "$pref" "$p"; else log "no smoke test for $p (SMOKE_ALL=1 runs it emulated)"; fi
  done
fi
echo "$REF" > "$OUT/image.tag"
printf '%s\n' "${built[@]}" > "$OUT/image.local"
printf '%s\n' "${platforms[@]}" > "$OUT/image.platforms"

if command -v syft >/dev/null 2>&1 || command -v trivy >/dev/null 2>&1; then
  for i in "${!built[@]}"; do
    img="${built[$i]}" p="${platforms[$i]}"
    # one platform: the established file names; several: suffixed per arch, the native
    # one (SBOM attested by push-image.sh) also under the plain names
    sfx=""; [[ ${#platforms[@]} -gt 1 ]] && sfx=".${p#linux/}"
    podman save --format docker-archive --output "$OUT/image.tar" "$img" >/dev/null
    if command -v syft >/dev/null 2>&1; then
      log "generating SBOM (SPDX + CycloneDX) for $p"
      syft "docker-archive:$OUT/image.tar" --quiet \
        --output "spdx-json=$OUT/sbom${sfx}.spdx.json" --output "cyclonedx-json=$OUT/sbom${sfx}.cdx.json"
    else
      log "syft not installed - no SBOM (make tools-supply-chain)"
    fi
    if command -v trivy >/dev/null 2>&1; then
      log "scanning for vulnerabilities ($p)"
      strict=0; [[ "${SCAN_STRICT:-0}" == "1" ]] && strict=1
      trivy image --input "$OUT/image.tar" --quiet --scanners vuln \
        --severity "${SCAN_SEVERITY:-HIGH,CRITICAL}" --ignore-unfixed \
        --format json --output "$OUT/trivy${sfx}.json" --exit-code "$strict" \
        || { [[ "$strict" == "1" ]] && die "vulnerabilities found (see $OUT/trivy${sfx}.json)"; log "trivy scan could not complete"; }
    else
      log "trivy not installed - no vulnerability scan (make tools-supply-chain)"
    fi
    rm -f "$OUT/image.tar"
    if [[ -n "$sfx" && "$p" == "$native" ]]; then
      for f in sbom.spdx.json sbom.cdx.json trivy.json; do
        [[ -f "$OUT/${f%%.*}${sfx}.${f#*.}" ]] && cp "$OUT/${f%%.*}${sfx}.${f#*.}" "$OUT/$f"
      done
    fi
  done
else
  log "syft/trivy not installed - skipping SBOM and scan (make tools-supply-chain)"
fi
log "built $REF"
echo "$REF"
