"""Declarative source specifications.

A source is a directory ``ingestion/sources/<name>/`` holding ``source.yml`` (entities, formats,
identifier fields, options) and an extractor callable that yields :class:`NativeFile` objects.
The framework owns everything else (landing layout, manifest, checksums, indexing).
"""

from __future__ import annotations

import importlib
import os
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

SUPPORTED_FORMATS = ("json", "jsonl", "csv", "xml", "parquet")
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
RESERVED_SOURCE_NAMES = ("iceberg",)  # prefix of the raw index tables in the raw bucket
SOURCES_DIR = Path(__file__).resolve().parent.parent / "sources"


class SpecError(ValueError):
    pass


@dataclass(frozen=True)
class EntitySpec:
    name: str
    format: str
    options: Mapping[str, Any] = field(default_factory=dict)
    # pseudonym name -> field (dotted path) inside one record that holds a person identifier
    identifiers: Mapping[str, str] = field(default_factory=dict)
    description: str = ""

    def __post_init__(self) -> None:
        if not _NAME.match(self.name):
            raise SpecError(f"entity name {self.name!r} must match {_NAME.pattern}")
        if self.format not in SUPPORTED_FORMATS:
            raise SpecError(f"entity {self.name}: unsupported format {self.format!r}")


@dataclass(frozen=True)
class NativeFile:
    """One file exactly as the source system delivered it."""

    entity: str
    file_name: str
    content: bytes
    record_count: int | None = None

    def __post_init__(self) -> None:
        if "/" in self.file_name or self.file_name.startswith("."):
            raise SpecError(f"illegal native file name {self.file_name!r}")


@dataclass
class ExtractContext:
    """Handed to the extractor. ``state`` is persisted by dlt together with a successful load."""

    source: SourceSpec
    load_id: str
    logical_date: str
    state: dict[str, Any]
    options: Mapping[str, Any]


Extractor = Callable[[ExtractContext], Iterable[NativeFile]]


@dataclass(frozen=True)
class SourceSpec:
    name: str
    description: str
    extractor: str
    entities: tuple[EntitySpec, ...]
    options: Mapping[str, Any] = field(default_factory=dict)
    owner: str = ""

    def __post_init__(self) -> None:
        if not _NAME.match(self.name):
            raise SpecError(f"source name {self.name!r} must match {_NAME.pattern}")
        if self.name in RESERVED_SOURCE_NAMES:
            raise SpecError(f"source name {self.name!r} is reserved")
        names = [e.name for e in self.entities]
        if len(set(names)) != len(names):
            raise SpecError(f"source {self.name}: duplicate entity names")
        if not self.entities:
            raise SpecError(f"source {self.name}: no entities")

    def entity(self, name: str) -> EntitySpec:
        for e in self.entities:
            if e.name == name:
                return e
        raise SpecError(f"source {self.name} has no entity {name!r}")

    def load_extractor(self) -> Extractor:
        module_name, _, func_name = self.extractor.partition(":")
        if not func_name:
            raise SpecError(f"extractor must be 'module:function', got {self.extractor!r}")
        return getattr(importlib.import_module(module_name), func_name)

    def resolved_options(self) -> dict[str, Any]:
        """Options from source.yml, overridable with ``INGEST_<SOURCE>_<OPTION>`` env variables."""
        resolved = dict(self.options)
        for key, default in self.options.items():
            raw = os.environ.get(f"INGEST_{self.name}_{key}".upper())
            if raw is not None:
                resolved[key] = type(default)(raw) if default is not None else raw
        return resolved


def source_from_dict(doc: Mapping[str, Any]) -> SourceSpec:
    try:
        entities = tuple(
            EntitySpec(
                name=e["name"],
                format=e["format"],
                options=e.get("options") or {},
                identifiers=e.get("identifiers") or {},
                description=e.get("description", ""),
            )
            for e in doc["entities"]
        )
        return SourceSpec(
            name=doc["name"],
            description=doc.get("description", ""),
            extractor=doc["extractor"],
            entities=entities,
            options=doc.get("options") or {},
            owner=doc.get("owner", ""),
        )
    except KeyError as exc:
        raise SpecError(f"source spec is missing key {exc}") from exc


def load_source(name: str, sources_dir: Path = SOURCES_DIR) -> SourceSpec:
    path = sources_dir / name / "source.yml"
    if not path.is_file():
        raise SpecError(f"unknown source {name!r} (no {path})")
    spec = source_from_dict(yaml.safe_load(path.read_text(encoding="utf-8")))
    if spec.name != name:
        raise SpecError(f"{path}: name {spec.name!r} does not match directory {name!r}")
    return spec


def list_sources(sources_dir: Path = SOURCES_DIR) -> list[SourceSpec]:
    return [load_source(p.parent.name, sources_dir) for p in sorted(sources_dir.glob("*/source.yml"))]
