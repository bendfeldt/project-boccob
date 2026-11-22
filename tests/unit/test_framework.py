import gzip
import json
import re
from datetime import UTC, datetime

import pytest

from ingestion.framework import formats
from ingestion.framework.indexing import INDEX_SCHEMA, ChecksumMismatchError, records_table, subject_keys
from ingestion.framework.manifest import ManifestEntry, landing_key, load_date_of, parse_manifest, sha256_hex
from ingestion.framework.pseudonym import normalise_identifier, pseudonymise
from ingestion.framework.spec import EntitySpec, NativeFile, SpecError, list_sources, load_source, source_from_dict

PEPPER = "unit-test-pepper-0123456789abcdef0123456789"


# --- formats -----------------------------------------------------------------------------------


def test_json_record_path_and_single_document():
    data = json.dumps({"meta": {}, "result": {"items": [{"a": 1}, {"a": 2}]}}).encode()
    assert list(formats.iter_records("json", data, {"record_path": "result.items"})) == ['{"a":1}', '{"a":2}']
    assert list(formats.iter_records("json", b'{"a": 1}')) == ['{"a":1}']
    with pytest.raises(formats.FormatError):
        list(formats.iter_records("json", data, {"record_path": "nope"}))


def test_csv_keeps_values_as_text_and_extra_cells():
    data = b"id;amount\r\n1;1.234,50\r\n2;7,00;surplus\r\n"
    rows = [json.loads(r) for r in formats.iter_records("csv", data, {"delimiter": ";"})]
    assert rows[0] == {"id": "1", "amount": "1.234,50"}
    assert rows[1]["_extra_cells"] == ["surplus"]


def test_xml_attributes_text_and_repeats():
    data = b"<root><rec id='7'><name>A</name><tag>x</tag><tag>y</tag><empty/></rec></root>"
    (rec,) = [json.loads(r) for r in formats.iter_records("xml", data, {"record_tag": "rec"})]
    assert rec == {"@id": "7", "name": "A", "tag": ["x", "y"], "empty": None}


def test_xml_rejects_entity_expansion():
    bomb = b'<?xml version="1.0"?><!DOCTYPE d [<!ENTITY a "aaaa">]><root><rec>&a;</rec></root>'
    with pytest.raises(Exception):
        list(formats.iter_records("xml", bomb, {"record_tag": "rec"}))


def test_jsonl_and_unknown_format():
    assert list(formats.iter_records("jsonl", b'{"a":1}\n\n{"a":2}\n')) == ['{"a":1}', '{"a":2}']
    with pytest.raises(formats.FormatError):
        list(formats.iter_records("yaml", b""))


def test_extract_field():
    rec = '{"a": {"b": " x "}, "n": 5, "o": {}, "e": ""}'
    assert formats.extract_field(rec, "a.b") == "x"
    assert formats.extract_field(rec, "n") == "5"
    assert formats.extract_field(rec, "o") is None
    assert formats.extract_field(rec, "e") is None
    assert formats.extract_field(rec, "missing.path") is None


# --- pseudonymisation --------------------------------------------------------------------------


def test_pseudonym_is_deterministic_keyed_and_hex():
    a = pseudonymise("610397-0407", PEPPER)
    assert a == pseudonymise("610397-0407", PEPPER)
    assert re.fullmatch(r"[0-9a-f]{64}", a)
    assert a != pseudonymise("610397-0407", PEPPER + "x"), "a different pepper must give a different key"
    assert a != pseudonymise("610397-0408", PEPPER)


def test_cpr_is_normalised_before_hashing():
    assert normalise_identifier(" 610397-0407 ") == "6103970407"
    assert pseudonymise("6103970407", PEPPER) == pseudonymise("610397-0407", PEPPER)
    assert normalise_identifier("abc-123") == "abc-123"


def test_empty_pepper_is_rejected():
    with pytest.raises(ValueError):
        pseudonymise("610397-0407", "")


# --- manifest ----------------------------------------------------------------------------------


def test_landing_key_follows_contract_layout():
    key = landing_key("borgerportal", "citizens", "1794855292.8192327", "citizens_1.json")
    assert re.fullmatch(r"borgerportal/citizens/load_date=\d{4}-\d{2}-\d{2}/1794855292\.8192327/citizens_1\.json", key)
    assert load_date_of("0") == "1970-01-01"


def _entry(content: bytes, **kw) -> ManifestEntry:
    base = {
        "source": "s",
        "entity": "citizens",
        "load_id": "1794855292.8",
        "load_date": "2026-11-16",
        "logical_date": "2026-11-16",
        "file_name": "f.json",
        "path": "s/citizens/load_date=2026-11-16/1794855292.8/f.json",
        "format": "json",
        "size_bytes": len(content),
        "sha256": sha256_hex(content),
        "record_count": 2,
        "landed_at": "2026-11-16T00:00:00+00:00",
    }
    base.update(kw)
    return ManifestEntry(**base)


def test_manifest_parses_plain_and_gzip_and_ignores_dlt_columns():
    row = {**_entry(b"x").as_row(), "_dlt_load_id": "1", "_dlt_id": "abc"}
    line = (json.dumps(row) + "\n").encode()
    assert parse_manifest(line) == [_entry(b"x")]
    assert parse_manifest(gzip.compress(line)) == [_entry(b"x")]


# --- index rows --------------------------------------------------------------------------------

CITIZENS = json.dumps({"data": [{"cpr": "610397-0407", "n": 1}, {"n": 2}]}).encode()
ENTITY = EntitySpec(name="citizens", format="json", options={"record_path": "data"}, identifiers={"cpr": "cpr"})


def test_records_table_schema_rows_and_subject_keys():
    now = datetime(2026, 1, 1, tzinfo=UTC)
    (table,) = list(records_table(_entry(CITIZENS), ENTITY, CITIZENS, PEPPER, now))
    assert table.schema.equals(INDEX_SCHEMA)
    rows = table.to_pylist()
    assert [r["_raw_row"] for r in rows] == [1, 2]
    assert json.loads(rows[0]["payload"]) == {"cpr": "610397-0407", "n": 1}
    assert json.loads(rows[0]["_subject_keys"]) == {"cpr": pseudonymise("610397-0407", PEPPER)}
    assert rows[1]["_subject_keys"] is None
    assert PEPPER not in json.dumps(rows, default=str)


def test_records_table_refuses_tampered_file():
    with pytest.raises(ChecksumMismatchError):
        list(records_table(_entry(CITIZENS), ENTITY, CITIZENS + b" ", PEPPER, datetime.now(UTC)))


def test_identifiers_require_a_pepper():
    with pytest.raises(RuntimeError):
        subject_keys('{"cpr": "1"}', ENTITY, None)
    assert subject_keys('{"cpr": "1"}', EntitySpec(name="x", format="json"), None) is None


# --- specs -------------------------------------------------------------------------------------


def test_demo_source_spec_loads():
    spec = load_source("borgerportal")
    assert [e.name for e in spec.entities] == ["citizens", "cases", "payments", "case_types"]
    assert {e.format for e in spec.entities} == {"json", "xml", "csv", "parquet"}
    assert callable(spec.load_extractor())
    assert spec in list_sources()


def test_spec_validation():
    with pytest.raises(SpecError):
        EntitySpec(name="Bad-Name", format="json")
    with pytest.raises(SpecError):
        EntitySpec(name="ok", format="xlsx")
    with pytest.raises(SpecError):
        NativeFile("e", "../evil.json", b"")
    with pytest.raises(SpecError):
        source_from_dict({"name": "s", "extractor": "m:f", "entities": []})
    with pytest.raises(SpecError):
        load_source("does_not_exist")
    with pytest.raises(SpecError):
        source_from_dict({"name": "iceberg", "extractor": "m:f", "entities": [{"name": "a", "format": "json"}]})


def test_options_can_be_overridden_from_env(monkeypatch):
    monkeypatch.setenv("INGEST_BORGERPORTAL_CITIZENS", "50")
    assert load_source("borgerportal").resolved_options()["citizens"] == 50


def test_indexed_load_ids_is_a_set_not_a_high_water_mark():
    from ingestion.framework.indexing import indexed_load_ids

    loads = {"1.0": [], "2.0": [], "3.0": []}
    # 3.0 was indexed before 2.0 committed (overlapping landings): 2.0 is still pending
    assert indexed_load_ids({"indexed_load_ids": ["1.0", "3.0"]}, loads) == {"1.0", "3.0"}
    old = {"last_indexed_load_id": "2.0"}  # state of the high-water mark version
    assert indexed_load_ids(old, loads) == {"1.0", "2.0"} and "last_indexed_load_id" not in old
