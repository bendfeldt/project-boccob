"""Object store helpers on top of s3fs that are aware of S3 object versioning.

The platform enables versioning on the lakehouse buckets. On a versioned bucket a plain
DeleteObject (what Trino, Polaris and s3fs do) only adds a delete marker; the bytes stay
retrievable as a noncurrent version. Everything that must make data unrecoverable (erasure,
retention, maintenance) therefore goes through the functions here, which list and delete object
versions explicitly. They also work on unversioned buckets (where every object has the single
version id ``null``).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

log = logging.getLogger(__name__)
DELETE_BATCH = 1000  # S3 DeleteObjects limit


@dataclass(frozen=True)
class ObjectVersion:
    key: str
    version_id: str  # "null" on unversioned buckets
    is_latest: bool
    is_delete_marker: bool
    last_modified: datetime
    size: int = 0


def _sync_call(fs: Any, method: str, **kwargs: Any) -> dict:
    from fsspec.asyn import sync

    return sync(fs.loop, fs._call_s3, method, **kwargs)


def iter_versions(fs: Any, bucket: str, prefix: str = "") -> Iterator[ObjectVersion]:
    """Every version and delete marker under ``prefix`` (paginated ListObjectVersions)."""
    kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
    while True:
        page = _sync_call(fs, "list_object_versions", **kwargs)
        for v in page.get("Versions", []):
            yield ObjectVersion(
                v["Key"],
                v.get("VersionId") or "null",
                bool(v.get("IsLatest")),
                False,
                v["LastModified"],
                int(v.get("Size", 0)),
            )
        for m in page.get("DeleteMarkers", []):
            yield ObjectVersion(
                m["Key"], m.get("VersionId") or "null", bool(m.get("IsLatest")), True, m["LastModified"]
            )
        if not page.get("IsTruncated"):
            return
        kwargs["KeyMarker"] = page.get("NextKeyMarker")
        if page.get("NextVersionIdMarker"):
            kwargs["VersionIdMarker"] = page["NextVersionIdMarker"]
        else:
            kwargs.pop("VersionIdMarker", None)


def read_version(fs: Any, bucket: str, version: ObjectVersion) -> bytes:
    kwargs: dict[str, Any] = {"Bucket": bucket, "Key": version.key}
    if version.version_id != "null":
        kwargs["VersionId"] = version.version_id
    from fsspec.asyn import sync

    async def _read() -> bytes:
        resp = await fs._call_s3("get_object", **kwargs)
        async with resp["Body"] as body:
            return await body.read()

    return sync(fs.loop, _read)


def _delete_versions(fs: Any, bucket: str, versions: list[ObjectVersion]) -> int:
    deleted = 0
    for start in range(0, len(versions), DELETE_BATCH):
        chunk = versions[start : start + DELETE_BATCH]
        objects = [{"Key": v.key, **({"VersionId": v.version_id} if v.version_id != "null" else {})} for v in chunk]
        resp = _sync_call(fs, "delete_objects", Bucket=bucket, Delete={"Objects": objects, "Quiet": True})
        errors = resp.get("Errors", [])
        if errors:
            first = errors[0]
            raise PermissionError(
                f"could not delete {len(errors)} object version(s) in {bucket}, e.g. {first.get('Key')}: "
                f"{first.get('Code')} {first.get('Message')}"
            )
        deleted += len(chunk)
    fs.invalidate_cache()
    return deleted


def delete_permanently(fs: Any, bucket: str, key: str) -> int:
    """Delete ``key`` with all its versions and delete markers. Returns the number of removed entries."""
    versions = [v for v in iter_versions(fs, bucket, key) if v.key == key]
    # data versions first: removing a delete marker first would briefly make an old version current
    versions.sort(key=lambda v: v.is_delete_marker)
    removed = _delete_versions(fs, bucket, versions) if versions else 0
    if fs.exists(f"{bucket}/{key}"):
        raise RuntimeError(f"object s3://{bucket}/{key} still exists after delete")
    return removed


def noncurrent_versions(versions: list[ObjectVersion], older_than: datetime | None = None) -> list[ObjectVersion]:
    """Select what a purge removes (pure function, unit tested).

    * every noncurrent version (an older version of a key, or any version of a deleted key) - with
      ``older_than``, only those that became noncurrent before that time
    * the delete marker of a deleted key once none of that key's data versions remain
    Current versions are never selected. Data versions come before delete markers in the result.
    """
    by_key: dict[str, list[ObjectVersion]] = {}
    for v in versions:
        by_key.setdefault(v.key, []).append(v)
    data: list[ObjectVersion] = []
    markers: list[ObjectVersion] = []
    for entries in by_key.values():
        entries.sort(key=lambda v: (v.last_modified, v.is_latest))
        key_data: list[ObjectVersion] = []
        key_markers: list[ObjectVersion] = []
        kept_noncurrent = False
        for i, v in enumerate(entries):
            if v.is_latest:
                continue
            # a version becomes noncurrent when the next entry of the same key is written
            superseded_at = entries[i + 1].last_modified if i + 1 < len(entries) else v.last_modified
            if older_than is not None and superseded_at >= older_than:
                kept_noncurrent = True
                continue
            (key_markers if v.is_delete_marker else key_data).append(v)
        latest = entries[-1]
        deleted_key = latest.is_latest and latest.is_delete_marker
        if deleted_key and not kept_noncurrent and (older_than is None or latest.last_modified < older_than):
            key_markers.append(latest)  # nothing of the key is left: drop its marker too
        data.extend(key_data)
        markers.extend(key_markers)
    return data + markers


def purge_noncurrent(fs: Any, bucket: str, prefix: str = "", older_than: datetime | None = None) -> int:
    """Permanently remove noncurrent object versions (and leftover delete markers) under ``prefix``.

    Needs s3:ListBucketVersions and s3:DeleteObjectVersion on the bucket; raises PermissionError
    otherwise, so a caller never reports data as gone when it is not.
    """
    selected = noncurrent_versions(list(iter_versions(fs, bucket, prefix)), older_than)
    if not selected:
        return 0
    removed = _delete_versions(fs, bucket, selected)
    log.info("purged %d noncurrent object version(s) in s3://%s/%s", removed, bucket, prefix)
    return removed


def enable_versioning(fs: Any, bucket: str) -> None:
    _sync_call(fs, "put_bucket_versioning", Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
