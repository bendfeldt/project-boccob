import json
import re

from ingestion.framework.formats import iter_records
from ingestion.sources.borgerportal import generator as g

SEED = 20260105


def test_generator_is_deterministic():
    a, b = g.simulate(SEED, 2, 60), g.simulate(SEED, 2, 60)
    assert g.citizens_json(a) == g.citizens_json(b)
    assert g.cases_xml(a) == g.cases_xml(b)
    assert g.payments_csv(a) == g.payments_csv(b)
    assert g.case_types_parquet(a) == g.case_types_parquet(b)


def test_different_seed_gives_different_data():
    assert g.citizens_json(g.simulate(1, 1, 60)) != g.citizens_json(g.simulate(2, 1, 60))


def test_cpr_numbers_are_synthetic_and_unique():
    batch = g.simulate(SEED, 1, 200)
    cprs = [c["cpr"] for c in batch.citizens]
    for cpr in cprs:
        assert re.fullmatch(r"\d{6}-\d{4}", cpr)
        assert 61 <= int(cpr[:2]) <= 88, "day part must be offset so it can never be a real CPR number"
    assert len(set(cprs)) == len(cprs) - 1  # exactly one in-file duplicate is delivered on purpose


def test_second_delivery_contains_changes_tombstones_and_late_members():
    first, second = g.simulate(SEED, 1, 200), g.simulate(SEED, 2, 200)
    first_cprs = {c["cpr"] for c in first.citizens}
    second_cprs = {c["cpr"] for c in second.citizens}
    assert first_cprs & second_cprs, "existing citizens must change (SCD2)"
    assert any(c["deleted"] for c in second.citizens), "deletes are delivered as tombstones"
    # citizens whose payments came first and whose master data arrives one delivery later
    paid_first = {p["cpr"] for p in first.payments} - first_cprs
    assert paid_first and paid_first <= second_cprs
    # late arriving facts: payment dated before the delivery's business date
    assert any(p["payment_date"] != second.business_date.strftime("%d-%m-%Y") for p in second.payments)
    assert len(second.case_types) == len(first.case_types) + 1


def test_native_formats_round_trip_through_the_splitters():
    batch = g.simulate(SEED, 1, 40)
    citizens = [json.loads(r) for r in iter_records("json", g.citizens_json(batch), {"record_path": "data"})]
    assert citizens == batch.citizens
    cases = [json.loads(r) for r in iter_records("xml", g.cases_xml(batch), {"record_tag": "case"})]
    assert [c["@id"] for c in cases] == [c["id"] for c in batch.cases]
    assert cases[0]["type"]["@code"] == batch.cases[0]["type"]
    payments = [json.loads(r) for r in iter_records("csv", g.payments_csv(batch), {"delimiter": ";"})]
    assert payments == batch.payments  # all values stay text, e.g. '1.234,56'
    types = [json.loads(r) for r in iter_records("parquet", g.case_types_parquet(batch))]
    assert types == batch.case_types


def test_sequence_must_be_positive():
    import pytest

    with pytest.raises(ValueError):
        g.simulate(SEED, 0)
