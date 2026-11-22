"""Schema-on-read record splitters.

Each splitter turns one native file into a stream of records rendered as JSON text. No column
names or types are assumed: values are kept as the source wrote them (CSV/XML values stay strings).
The native file in the raw bucket remains the untouched source of truth; the rendering only makes
the records addressable from SQL (``json_extract_scalar``) in the clean layer.
"""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Iterator, Mapping
from typing import IO, Any

from defusedxml import ElementTree as SafeET


class FormatError(ValueError):
    pass


def _dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


def _walk(doc: Any, path: str | None) -> Any:
    if not path:
        return doc
    for part in path.split("."):
        if not isinstance(doc, Mapping) or part not in doc:
            raise FormatError(f"record_path segment {part!r} not found")
        doc = doc[part]
    return doc


def _text(data: IO[bytes], options: Mapping[str, Any], default: str = "utf-8") -> io.TextIOWrapper:
    return io.TextIOWrapper(data, encoding=options.get("encoding", default), newline="")


def _json(data: IO[bytes], options: Mapping[str, Any]) -> Iterator[str]:
    # shortcut: a JSON document is parsed whole (no streaming parser installed); use jsonl for big feeds
    doc = json.load(_text(data, options))
    records = _walk(doc, options.get("record_path"))
    if isinstance(records, list):
        for rec in records:
            yield _dumps(rec)
    else:
        yield _dumps(records)


def _jsonl(data: IO[bytes], options: Mapping[str, Any]) -> Iterator[str]:
    for line in _text(data, options):
        if line.strip():
            yield line.strip()


def _csv(data: IO[bytes], options: Mapping[str, Any]) -> Iterator[str]:
    reader = csv.DictReader(
        _text(data, options, "utf-8-sig"),
        delimiter=options.get("delimiter", ","),
        quotechar=options.get("quotechar", '"'),
    )
    for row in reader:
        extra = row.pop(None, None)  # more cells than headers: keep, do not drop silently
        if extra:
            row["_extra_cells"] = extra
        yield _dumps(row)


def _element_to_obj(elem: Any) -> Any:
    children = list(elem)
    if not children and not elem.attrib:
        return (elem.text or "").strip() or None
    obj: dict[str, Any] = {f"@{k}": v for k, v in elem.attrib.items()}
    text = (elem.text or "").strip()
    if text:
        obj["#text"] = text
    for child in children:
        value = _element_to_obj(child)
        if child.tag in obj:
            if not isinstance(obj[child.tag], list):
                obj[child.tag] = [obj[child.tag]]
            obj[child.tag].append(value)
        else:
            obj[child.tag] = value
    return obj


def _xml(data: IO[bytes], options: Mapping[str, Any]) -> Iterator[str]:
    tag = options.get("record_tag")
    if not tag:
        raise FormatError("xml entities need options.record_tag")
    for _, elem in SafeET.iterparse(data, events=("end",)):
        if elem.tag == tag:
            yield _dumps(_element_to_obj(elem))
            elem.clear()  # streaming: a processed record is not kept in memory


def _parquet(data: IO[bytes], options: Mapping[str, Any]) -> Iterator[str]:
    import pyarrow.parquet as pq

    for batch in pq.ParquetFile(data).iter_batches():
        for row in batch.to_pylist():
            yield _dumps(row)


_SPLITTERS = {"json": _json, "jsonl": _jsonl, "csv": _csv, "xml": _xml, "parquet": _parquet}


def iter_records(fmt: str, data: bytes | IO[bytes], options: Mapping[str, Any] | None = None) -> Iterator[str]:
    """Yield every record of a native file (bytes or a binary file object) as JSON text, in file order."""
    try:
        splitter = _SPLITTERS[fmt]
    except KeyError:
        raise FormatError(f"unsupported format {fmt!r}") from None
    yield from splitter(io.BytesIO(data) if isinstance(data, bytes) else data, options or {})


def extract_field(record_json: str, path: str) -> str | None:
    """Read a (dotted-path) field from a rendered record; ``None`` when absent/empty."""
    doc: Any = json.loads(record_json)
    for part in path.split("."):
        if not isinstance(doc, Mapping) or part not in doc:
            return None
        doc = doc[part]
    if doc is None or isinstance(doc, (dict, list)):
        return None
    text = str(doc).strip()
    return text or None
