"""Load manifests: the audit record and commit marker of a landed load."""

from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

MANIFEST_TABLE = "_manifest"


@dataclass(frozen=True)
class ManifestEntry:
    source: str
    entity: str
    load_id: str
    load_date: str
    logical_date: str
    file_name: str
    path: str  # object key relative to the raw bucket
    format: str
    size_bytes: int
    sha256: str
    record_count: int | None
    landed_at: str

    def as_row(self) -> dict[str, Any]:
        return asdict(self)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_date_of(load_id: str) -> str:
    """dlt load ids are unix timestamps; the landing partition is the UTC date of the load."""
    return datetime.fromtimestamp(float(load_id), tz=UTC).strftime("%Y-%m-%d")


def landing_key(source: str, entity: str, load_id: str, file_name: str) -> str:
    """Contract layout: <source>/<entity>/load_date=YYYY-MM-DD/<load_id>/<file>."""
    return f"{source}/{entity}/load_date={load_date_of(load_id)}/{load_id}/{file_name}"


def manifest_glob(source: str, load_id: str = "*") -> str:
    return f"{source}/{MANIFEST_TABLE}/{load_id}.*.jsonl"


def parse_manifest(data: bytes) -> list[ManifestEntry]:
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    fields = ManifestEntry.__dataclass_fields__
    entries = []
    for line in data.decode("utf-8").splitlines():
        if line.strip():
            row = json.loads(line)
            entries.append(ManifestEntry(**{k: row.get(k) for k in fields}))
    return entries


def read_manifests(fs: Any, bucket: str, source: str, load_id: str = "*") -> dict[str, list[ManifestEntry]]:
    """All committed loads of a source: ``{load_id: [entries]}`` ordered by load id."""
    loads: dict[str, list[ManifestEntry]] = {}
    for path in sorted(fs.glob(f"{bucket}/{manifest_glob(source, load_id)}")):
        for entry in parse_manifest(fs.cat_file(path)):
            loads.setdefault(entry.load_id, []).append(entry)
    return dict(sorted(loads.items(), key=lambda kv: float(kv[0])))
