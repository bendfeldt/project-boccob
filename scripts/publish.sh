#!/usr/bin/env bash
# Build and publish a release of this repository to an environment's registry:
# the code location image and the OPA bundle, then print the two digest-pinned references.
#
#   BOCCOB_PLATFORM_ENV=<file> make publish
#
# Registry, its CA and the push credential come from the platform environment file
# (docs/platform-contract.md, section 4). Signing is a platform function (it holds the key):
# hand the printed references to `make sign REFS="<image ref> <bundle ref>"` in the platform
# repository, then set them as the environment's image / bundle variables.
#
#   VERSION            artifact version (default 0.1.0-<git describe>)
#   SKIP_BUILD=1       push what was built before
#   KEEP_LOCAL_IMAGE=1 keep the local image after the push (default: removed, saves disk)
#   BUILD_STORAGE_ROOT=<dir>  separate podman storage (and TMPDIR) for the build, e.g. a tmpfs on a
#                      disk-constrained host; removed afterwards
#   BUILD_PROXY / BUILD_NETWORK / BUILD_CA_BUNDLE  default from HTTPS_PROXY / host / SSL_CERT_FILE
#                      when an outbound proxy is configured (see scripts/build-image.sh)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need podman
need oras
load_platform_env BOCCOB_REGISTRY_HOST BOCCOB_REGISTRY_CA_FILE BOCCOB_REGISTRY_USER BOCCOB_REGISTRY_PASSWORD_FILE
[[ -r "$BOCCOB_REGISTRY_PASSWORD_FILE" ]] || die "registry password file $BOCCOB_REGISTRY_PASSWORD_FILE is not readable"

REGISTRY="$BOCCOB_REGISTRY_HOST" REGISTRY_CA_FILE="$BOCCOB_REGISTRY_CA_FILE" REGISTRY_USERNAME="$BOCCOB_REGISTRY_USER"
REGISTRY_PASSWORD="$(cat "$BOCCOB_REGISTRY_PASSWORD_FILE")"
VERSION="$(artifact_version)"
export REGISTRY REGISTRY_CA_FILE REGISTRY_USERNAME REGISTRY_PASSWORD VERSION
if [[ -n "${HTTPS_PROXY:-}" ]]; then  # the image build fetches packages through the outbound proxy
  export BUILD_PROXY="${BUILD_PROXY:-$HTTPS_PROXY}" BUILD_NETWORK="${BUILD_NETWORK:-host}"
  [[ -n "${BUILD_CA_BUNDLE:-}" || -z "${SSL_CERT_FILE:-}" ]] || export BUILD_CA_BUNDLE="$SSL_CERT_FILE"
fi

if [[ -n "${BUILD_STORAGE_ROOT:-}" ]]; then
  [[ "${SKIP_BUILD:-0}" != "1" ]] || die "BUILD_STORAGE_ROOT is removed after each run: SKIP_BUILD=1 has nothing to push"
  mkdir -p "$BUILD_STORAGE_ROOT"/{storage,run,tmp}
  printf '[storage]\ndriver = "overlay"\ngraphroot = "%s/storage"\nrunroot = "%s/run"\n' \
    "$BUILD_STORAGE_ROOT" "$BUILD_STORAGE_ROOT" > "$BUILD_STORAGE_ROOT/storage.conf"
  export CONTAINERS_STORAGE_CONF="$BUILD_STORAGE_ROOT/storage.conf" TMPDIR="$BUILD_STORAGE_ROOT/tmp"
  trap 'podman rmi -a -f >/dev/null 2>&1 || true; rm -rf -- "${BUILD_STORAGE_ROOT:?}"/{storage,run,tmp,storage.conf}' EXIT
fi

[[ "${SKIP_BUILD:-0}" == "1" ]] || "$ROOT/scripts/build-image.sh" >/dev/null
"$ROOT/scripts/push-image.sh" >/dev/null
"$ROOT/scripts/push-opa-bundle.sh" >/dev/null
image_ref="$(cat "$ROOT/dist/image/image.ref")" bundle_ref="$(cat "$ROOT/dist/opa/bundle.ref")"
[[ "$image_ref" == "$REGISTRY/"*@sha256:* && "$bundle_ref" == "$REGISTRY/"*@sha256:* ]] \
  || die "unexpected artifact references: $image_ref $bundle_ref"
if [[ "${KEEP_LOCAL_IMAGE:-0}" != "1" && -z "${BUILD_STORAGE_ROOT:-}" ]]; then
  podman rmi "$(cat "$ROOT/dist/image/image.tag")" >/dev/null 2>&1 || true
fi
log "published $VERSION - sign them in the platform repository: make sign REFS=\"$image_ref $bundle_ref\""
printf 'dagster_code_location_image = "%s"\nopa_bundle_ref              = "%s"\n' "$image_ref" "$bundle_ref"
