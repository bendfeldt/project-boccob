#!/usr/bin/env bash
# Push the code location image to the OCI registry (Zot) and record its digest reference.
# Stable interface (called by the IaC `make release-publish`; documented in docs/releases.md):
#   REGISTRY (required), REGISTRY_NAMESPACE, VERSION  - as in build-image.sh
#   REGISTRY_CA_FILE            CA (PEM) of the registry certificate; passed as a temporary
#                               --cert-dir to podman and via SSL_CERT_FILE to cosign (no /etc writes)
#   REGISTRY_HOST               host[:port] added to NO_PROXY/no_proxy for this run
#                               (default: the host part of REGISTRY)
#   REGISTRY_TLS_VERIFY=false   only for a local test registry without TLS
#   REGISTRY_USERNAME / REGISTRY_PASSWORD  optional login (otherwise `podman login` beforehand)
#   COSIGN_KEY                  optional: sign + attach the SBOM here. On the platform signing is
#                               done by the IaC (`make sign`, OpenBao transit key) - leave it unset
# Result: dist/image/image.ref = <registry>/<ns>/dagster-code-location@sha256:...
#   single-platform build: the image manifest digest; multi-platform build (PLATFORMS with
#   several entries, see build-image.sh): the digest of the pushed INDEX (manifest list)
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
multi=false
if podman manifest exists "$REF" 2>/dev/null; then multi=true
else podman image exists "$REF" || die "image $REF not built - run scripts/build-image.sh with the same REGISTRY/VERSION"; fi

tls=(--tls-verify="${REGISTRY_TLS_VERIFY:-true}" "${REGISTRY_PODMAN_ARGS[@]}")
if [[ -n "${REGISTRY_USERNAME:-}" ]]; then
  printf '%s' "${REGISTRY_PASSWORD:?}" | podman login "${tls[@]}" -u "$REGISTRY_USERNAME" --password-stdin "$REGISTRY"
fi
if $multi; then
  log "pushing manifest list $REF ($(podman manifest inspect "$REF" | jq -r '[.manifests[].platform | "\(.os)/\(.architecture)"] | join(", ")'))"
  podman manifest push "${tls[@]}" --all --digestfile "$OUT/image.digest" "$REF" "docker://$REF"
else
  podman push "${tls[@]}" --digestfile "$OUT/image.digest" "$REF"
fi
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
