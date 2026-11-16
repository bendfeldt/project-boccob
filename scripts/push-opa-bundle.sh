#!/usr/bin/env bash
# Publish the OPA bundle as an OCI artifact (the layout OPA's OCI bundle downloader expects).
#   REGISTRY           registry host[:port], e.g. zot.dev.lakehouse.internal   (required)
#   REGISTRY_NAMESPACE repository prefix, default "boccob"
#   REGISTRY_PLAIN_HTTP=1 for a registry without TLS (local testing only)
#   credentials: `oras login` beforehand, or REGISTRY_USERNAME / REGISTRY_PASSWORD
# Result: <REGISTRY>/<REGISTRY_NAMESPACE>/opa-policy-bundle:<VERSION>  (digest in dist/opa/bundle.ref)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need oras
: "${REGISTRY:?set REGISTRY to the OCI registry (Zot)}"

VERSION="$(artifact_version)"
REF="${REGISTRY}/${REGISTRY_NAMESPACE:-boccob}/opa-policy-bundle:${VERSION}"
OUT="$ROOT/dist/opa"
[[ -f "$OUT/bundle.tar.gz" ]] || "$ROOT/scripts/build-opa-bundle.sh" >/dev/null

flags=()
[[ "${REGISTRY_PLAIN_HTTP:-0}" == "1" ]] && flags+=(--plain-http)
if [[ -n "${REGISTRY_USERNAME:-}" ]]; then
  printf '%s' "${REGISTRY_PASSWORD:?}" | oras login "${flags[@]}" -u "$REGISTRY_USERNAME" --password-stdin "$REGISTRY"
fi

cd "$OUT"
echo '{}' > config.json
oras push "${flags[@]}" "$REF" \
  --config config.json:application/vnd.oci.image.config.v1+json \
  --annotation "org.opencontainers.image.version=${VERSION}" \
  --annotation "org.opencontainers.image.title=boccob-opa-policy-bundle" \
  --export-manifest manifest.json \
  bundle.tar.gz:application/vnd.oci.image.layer.v1.tar+gzip
digest="sha256:$(sha256sum manifest.json | awk '{print $1}')"
echo "${REF%:*}@${digest}" > bundle.ref
log "pushed $REF ($digest)"
cat bundle.ref
