#!/usr/bin/env bash
# dev only: remove ALL lakehouse data of an environment so a release can start from scratch.
#   1. drop every view, table and schema of the four layer catalogs (as the pipeline service
#      account, from the code location pod; Iceberg drops purge the data files)
#   2. delete every object version and delete marker in the four lakehouse buckets (landing files,
#      manifests and dlt state included), with the purge credential: the Kubernetes Secret named
#      by the code location's PURGE_CREDENTIALS_SECRET, handed to the pod on stdin
# The keystore is lost with everything else: never outside dev.
#
#   CONFIRM=dev BOCCOB_PLATFORM_ENV=<file> make reset-dev-data
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need kubectl
need jq
load_platform_env BOCCOB_ENV BOCCOB_KUBECONFIG BOCCOB_DAGSTER_NAMESPACE BOCCOB_CODE_LOCATION
[[ "$BOCCOB_ENV" == "dev" ]] || die "dev only"
[[ "${CONFIRM:-}" == "$BOCCOB_ENV" ]] || die "set CONFIRM=$BOCCOB_ENV: this deletes all lakehouse data of $BOCCOB_ENV"

pod_py() { kc exec -i "$BOCCOB_CODE_LOCATION" -- python -I -c "$1"; }

log "dropping every view, table and schema of the layer catalogs"
pod_py '
from governance.config import catalog_of
from lakehouse.settings import LAYERS
from lakehouse.trino_client import TrinoClient, fqn, ident

trino = TrinoClient(source="boccob-reset-dev-data")
for layer in LAYERS:
    catalog = catalog_of(layer)
    for (schema,) in trino.execute(f"show schemas from {ident(catalog)}"):
        if schema in ("information_schema", "system"):
            continue
        rows = trino.execute(
            f"select table_name, table_type from {ident(catalog)}.information_schema.tables where table_schema = ?",
            [schema],
        )
        for name, kind in rows:
            what = "view" if kind == "VIEW" else "table"
            trino.execute(f"drop {what} if exists {fqn(catalog, schema, name)}")
        trino.execute(f"drop schema if exists {ident(catalog)}.{ident(schema)}")
        print(f"dropped {catalog}.{schema} ({len(rows)} objects)")
'

log "deleting every object version in the lakehouse buckets"
secret="$(pod_py 'import os; print(os.environ.get("PURGE_CREDENTIALS_SECRET", ""))')"
if [[ -n "$secret" ]]; then kc get secret "$secret" -o json | jq -c '.data | map_values(@base64d)'; else echo '{}'; fi \
  | kc exec -i "$BOCCOB_CODE_LOCATION" -- python -I -c '
import json, os, sys
from lakehouse.objectstore import _delete_versions, iter_versions
from lakehouse.settings import LAYERS, S3Settings, layer_bucket

os.environ.update(json.loads(sys.stdin.readline()))  # PURGE_S3_* of the purge Secret, if any
s3 = S3Settings.for_purge()
fs = s3.filesystem()
for layer in LAYERS:
    bucket = s3.raw_bucket if layer == "raw" else layer_bucket(layer)
    versions = sorted(iter_versions(fs, bucket), key=lambda v: v.is_delete_marker)  # data first
    removed = _delete_versions(fs, bucket, versions) if versions else 0
    print(f"{bucket}: {removed} object versions / delete markers removed")
'
log "lakehouse data of $BOCCOB_ENV reset - run the Dagster job migrate (or transform) next"
