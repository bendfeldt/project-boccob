#!/usr/bin/env bash
# Push the code location image to the OCI registry (Zot) and record its digest reference.
#   REGISTRY (required), REGISTRY_NAMESPACE, VERSION  - as in build-image.sh
#   REGISTRY_CA_FILE            CA (PEM) of the registry certificate (temporary --cert-dir, no /etc writes)
#   REGISTRY_HOST               host[:port] added to NO_PROXY/no_proxy (default: the host part of REGISTRY)
#   REGISTRY_TLS_VERIFY=false   only for a local test registry without TLS
#   REGISTRY_USERNAME / REGISTRY_PASSWORD  login into a temporary auth file, removed afterwards
# Result: dist/image/image.ref = <registry>/<ns>/dagster-code-location@sha256:...
# Signing is a platform function (it holds the key); this script only publishes.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need podman
: "${REGISTRY:?set REGISTRY to the OCI registry (Zot)}"

VERSION="$(artifact_version)"
REPO="${REGISTRY}/${REGISTRY_NAMESPACE:-boccob}/dagster-code-location"
REF="${REPO}:${VERSION}"
OUT="$ROOT/dist/image"
mkdir -p "$OUT"
registry_trust
podman image exists "$REF" || die "image $REF not built - run scripts/build-image.sh with the same REGISTRY/VERSION"

tls=(--tls-verify="${REGISTRY_TLS_VERIFY:-true}" "${REGISTRY_PODMAN_ARGS[@]}")
if [[ -n "${REGISTRY_USERNAME:-}" ]]; then
  printf '%s' "${REGISTRY_PASSWORD:?}" | podman login "${tls[@]}" -u "$REGISTRY_USERNAME" --password-stdin "$REGISTRY" >/dev/null
fi
podman push "${tls[@]}" --digestfile "$OUT/image.digest" "$REF"
DIGEST_REF="${REPO}@$(cat "$OUT/image.digest")"
echo "$DIGEST_REF" > "$OUT/image.ref"
log "pushed $DIGEST_REF"
echo "$DIGEST_REF"
