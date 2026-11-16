#!/usr/bin/env bash
# Push the code location image to the OCI registry (Zot) and record its digest reference.
#   REGISTRY (required), REGISTRY_NAMESPACE, VERSION  - as in build-image.sh
#   REGISTRY_TLS_VERIFY=false   only for a local test registry without TLS
#   REGISTRY_USERNAME / REGISTRY_PASSWORD  optional login (otherwise `podman login` beforehand)
#   COSIGN_KEY                  when set (and cosign is installed) the image is signed and the
#                               SBOM is attached as an attestation
# Result: dist/image/image.ref = <registry>/<ns>/dagster-code-location@sha256:...
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need podman
: "${REGISTRY:?set REGISTRY to the OCI registry (Zot)}"

VERSION="$(artifact_version)"
REPO="${REGISTRY}/${REGISTRY_NAMESPACE:-boccob}/dagster-code-location"
REF="${REPO}:${VERSION}"
OUT="$ROOT/dist/image"
mkdir -p "$OUT"
podman image exists "$REF" || die "image $REF not built - run scripts/build-image.sh with the same REGISTRY/VERSION"

tls=(--tls-verify="${REGISTRY_TLS_VERIFY:-true}")
if [[ -n "${REGISTRY_USERNAME:-}" ]]; then
  printf '%s' "${REGISTRY_PASSWORD:?}" | podman login "${tls[@]}" -u "$REGISTRY_USERNAME" --password-stdin "$REGISTRY"
fi
podman push "${tls[@]}" --digestfile "$OUT/image.digest" "$REF"
DIGEST_REF="${REPO}@$(cat "$OUT/image.digest")"
echo "$DIGEST_REF" > "$OUT/image.ref"

if [[ -n "${COSIGN_KEY:-}" ]] && command -v cosign >/dev/null 2>&1; then
  log "signing $DIGEST_REF"
  cosign sign --yes --key "$COSIGN_KEY" "$DIGEST_REF"
  [[ -f "$OUT/sbom.spdx.json" ]] && cosign attest --yes --key "$COSIGN_KEY" --type spdxjson \
    --predicate "$OUT/sbom.spdx.json" "$DIGEST_REF"
else
  log "COSIGN_KEY not set or cosign missing - image not signed"
fi
log "pushed $DIGEST_REF"
echo "$DIGEST_REF"
