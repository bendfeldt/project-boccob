"""Object store helpers on top of s3fs: permanent deletion that also removes object versions."""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


def _sync_call(fs: Any, method: str, **kwargs: Any) -> dict:
    from fsspec.asyn import sync

    return sync(fs.loop, fs._call_s3, method, **kwargs)


def delete_permanently(fs: Any, bucket: str, key: str) -> int:
    """Delete ``key`` including all versions and delete markers. Returns the number of removed versions.

    On buckets without versioning this is a plain delete (returns 1 when the object existed).
    """
    removed = 0
    try:
        listing = _sync_call(fs, "list_object_versions", Bucket=bucket, Prefix=key)
        entries = [v for v in listing.get("Versions", []) + listing.get("DeleteMarkers", []) if v["Key"] == key]
    except Exception as exc:  # store without the versioning API
        log.debug("list_object_versions unavailable (%s); falling back to plain delete", exc)
        entries = []
    for entry in entries:
        version = entry.get("VersionId")
        if version and version != "null":
            _sync_call(fs, "delete_object", Bucket=bucket, Key=key, VersionId=version)
            removed += 1
    path = f"{bucket}/{key}"
    fs.invalidate_cache()
    if fs.exists(path):
        fs.rm_file(path)
        removed += 1
    fs.invalidate_cache()
    if fs.exists(path):
        raise RuntimeError(f"object s3://{path} still exists after delete")
    return removed
