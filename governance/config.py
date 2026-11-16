"""Governance configuration: retention policy file + catalog names from the environment."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO = Path(__file__).resolve().parents[1]
RETENTION_FILE = Path(os.environ.get("RETENTION_CONFIG", REPO / "governance" / "retention.yml"))
_DURATION = re.compile(r"^\d+(s|m|h|d)$")


def catalog_of(layer: str) -> str:
    """Trino catalog of a layer (RAW_CATALOG, CLEAN_CATALOG, ... default = layer name)."""
    return os.environ.get(f"{layer.upper()}_CATALOG", layer)


def manifest_path() -> Path:
    project = Path(os.environ.get("DBT_PROJECT_DIR", REPO / "transform"))
    return Path(os.environ.get("DBT_MANIFEST_PATH", project / "target" / "manifest.json"))


@dataclass(frozen=True)
class LayerRetention:
    snapshot_retention: str
    orphan_file_retention: str
    landing_days: int | None = None
    uncommitted_landing_days: int | None = None
    index_days: int | None = None


@dataclass(frozen=True)
class RetentionConfig:
    layers: dict[str, LayerRetention]
    optimize_file_size_threshold: str
    exclude_schemas: tuple[str, ...]


def load_retention(path: Path = RETENTION_FILE) -> RetentionConfig:
    doc: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    layers = {}
    for name, cfg in doc["layers"].items():
        for key in ("snapshot_retention", "orphan_file_retention"):
            if not _DURATION.match(str(cfg[key])):
                raise ValueError(f"{path}: {name}.{key} must look like 7d / 12h, got {cfg[key]!r}")
        layers[name] = LayerRetention(**cfg)
    maint = doc.get("maintenance", {})
    threshold = str(maint.get("optimize_file_size_threshold", "64MB"))
    if not re.match(r"^\d+(kB|MB|GB)$", threshold):
        raise ValueError(f"{path}: invalid optimize_file_size_threshold {threshold!r}")
    return RetentionConfig(
        layers=layers,
        optimize_file_size_threshold=threshold,
        exclude_schemas=tuple(maint.get("exclude_schemas", ("information_schema", "system"))),
    )
