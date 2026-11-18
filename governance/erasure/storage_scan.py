"""Deep verification: prove that an erased subject is physically gone from object storage.

Reads every object version (current and noncurrent) of the given buckets and searches it for the
needles (identifier, pseudonym).
Parquet files are decoded column by column, Avro files (Iceberg manifests, which carry column
min/max bounds truncated to 16 characters) record by record; everything else is searched as bytes.
This is O(size of the lake) and meant for erasure verification and tests, not for routine use.
"""

from __future__ import annotations

import contextlib
import gzip
import io
import logging
import tempfile
from typing import Any

log = logging.getLogger(__name__)
BOUND_PREFIX = 16  # Iceberg truncates string bounds in manifests to this many characters


def _parquet_hits(data: bytes, needles: list[str]) -> bool:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    table = pq.read_table(io.BytesIO(data))
    for column in table.columns:
        if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
            for needle in needles:
                if pc.any(pc.match_substring(column, needle)).as_py():
                    return True
    return False


def _avro_hits(data: bytes, needles: list[str]) -> bool:
    from pyiceberg.avro.file import AvroFile
    from pyiceberg.io.pyarrow import PyArrowFileIO

    prefixes = {n[:BOUND_PREFIX] for n in needles} | set(needles)
    with tempfile.NamedTemporaryFile(suffix=".avro") as tmp:
        tmp.write(data)
        tmp.flush()
        with AvroFile(PyArrowFileIO().new_input(tmp.name)) as reader:
            for record in reader:
                text = repr(record)
                if any(p in text for p in prefixes):
                    return True
    return False


def _bytes_hit(data: bytes, needles: list[str]) -> bool:
    if data[:2] == b"\x1f\x8b":
        with contextlib.suppress(OSError):
            data = gzip.decompress(data)
    return any(n.encode("utf-8") in data for n in needles)


def object_contains(path: str, data: bytes, needles: list[str]) -> bool:
    try:
        if path.endswith(".parquet"):
            return _parquet_hits(data, needles) or _bytes_hit(data, needles)
        if path.endswith(".avro"):
            return _avro_hits(data, needles) or _bytes_hit(data, needles)
    except Exception as exc:  # undecodable file: fall back to the byte search
        log.warning("could not decode %s (%s); searching raw bytes", path, exc)
    return _bytes_hit(data, needles)


def scan_storage(fs: Any, buckets: list[str], needles: list[str], exclude: list[str] | None = None) -> list[str]:
    """Return every object version (current or noncurrent) that still contains one of the needles.

    Noncurrent versions are included: on a versioned bucket a deleted object is still readable
    by version id. Hits are reported as ``s3://bucket/key?versionId=...``. ``exclude`` lists
    ``bucket/key-prefix`` strings whose objects are not inspected (the erasure queue itself).
    """
    from lakehouse.objectstore import iter_versions, read_version

    needles = [n for n in needles if n]
    hits = []
    for bucket in buckets:
        for version in iter_versions(fs, bucket):
            if version.is_delete_marker or any(f"{bucket}/{version.key}".startswith(e) for e in exclude or ()):
                continue
            if object_contains(version.key, read_version(fs, bucket, version), needles):
                hits.append(f"s3://{bucket}/{version.key}?versionId={version.version_id}")
    return hits
