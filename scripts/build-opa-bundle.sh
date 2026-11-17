#!/usr/bin/env bash
# Build the OPA policy bundle (policies + config + generated classification data).
#   output: dist/opa/bundle.tar.gz  (+ bundle.tar.gz.sha256)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need opa

VERSION="$(artifact_version)"
OUT="$ROOT/dist/opa"
mkdir -p "$OUT"

opa check --strict "$ROOT/governance/policies"
(cd "$ROOT/governance/policies" && opa build --bundle . --ignore '*_test.rego' \
  --revision "$VERSION" --output "$OUT/bundle.tar.gz")
chmod 0644 "$OUT/bundle.tar.gz"
(cd "$OUT" && sha256sum bundle.tar.gz > bundle.tar.gz.sha256)
log "built $OUT/bundle.tar.gz (revision $VERSION)"
echo "$OUT/bundle.tar.gz"
