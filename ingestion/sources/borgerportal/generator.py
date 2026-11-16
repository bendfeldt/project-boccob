"""Deterministic synthetic data for the fictional "Borgerportal" system.

``generate(seed, sequence, citizens)`` returns the files of delivery number ``sequence``. The same
arguments always give byte-identical files. Deliveries build on each other so that downstream logic
is genuinely exercised:

* citizens move / change e-mail between deliveries (SCD2 versions), some are deleted (tombstones)
* payments arrive late (payment date before earlier deliveries)
* some payments reference a citizen whose master record is delivered one batch later
  (late-arriving dimension member -> inferred member in the keystore)
* a few records are delivered twice in a file (dedupe in clean)

All persons are invented. CPR numbers are shaped like ``DDMMYY-SSSS`` but the day part is offset by
60 (61-88), which is never a calendar date, so no generated number can equal a real CPR number.
E-mail uses the reserved ``.example`` TLD, phone numbers the unassigned ``+45 00`` range.
"""

from __future__ import annotations

import csv
import io
import json
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from xml.etree import ElementTree as ET  # noqa: S405 - only used to *write* XML

FIRST_NAMES = ["Anne", "Peter", "Mette", "Lars", "Sofie", "Jens", "Freja", "Mads", "Ida", "Søren",
               "Emma", "Niels", "Karen", "Rasmus", "Lærke", "Henrik", "Astrid", "Mikkel", "Signe", "Åge"]
LAST_NAMES = ["Jensen", "Nielsen", "Hansen", "Pedersen", "Andersen", "Christensen", "Larsen",
              "Sørensen", "Rasmussen", "Jørgensen", "Petersen", "Madsen", "Kristensen", "Olsen", "Thomsen"]
STREETS = ["Skovvej", "Engvej", "Birkevej", "Møllevej", "Kirkevej", "Strandvejen", "Åboulevarden",
           "Søndergade", "Nørregade", "Vestergade", "Østerbrogade", "Lærkevej"]
PLACES = [("8000", "Aarhus C", "0751"), ("5000", "Odense C", "0461"), ("9000", "Aalborg", "0851"),
          ("2100", "København Ø", "0101"), ("6700", "Esbjerg", "0561"), ("7100", "Vejle", "0630"),
          ("4000", "Roskilde", "0265"), ("8600", "Silkeborg", "0740")]
CASE_TYPES = [("BYG", "Byggesag", "Teknik og Miljø"), ("BOL", "Boligstøtte", "Borgerservice"),
              ("DAG", "Dagtilbud", "Børn og Unge"), ("PAS", "Pas og kørekort", "Borgerservice"),
              ("HJM", "Hjælpemidler", "Sundhed og Omsorg"), ("SKA", "Skat og opkrævning", "Økonomi")]
NEW_CASE_TYPE = ("FLY", "Flytteanmeldelse", "Borgerservice")
BASE_DATE = date(2026, 1, 5)


@dataclass
class Citizen:
    cpr: str
    first_name: str
    last_name: str
    street: str
    house_no: str
    postal_code: str
    city: str
    municipality_code: str
    email: str
    phone: str
    birth_date: str
    updated_at: str
    deleted: bool = False
    delivered: bool = False  # master record already sent to the lakehouse


@dataclass
class World:
    seed: int
    citizens: dict[str, Citizen] = field(default_factory=dict)
    cases: dict[str, dict] = field(default_factory=dict)
    payment_seq: int = 0
    case_seq: int = 0


@dataclass
class Batch:
    sequence: int
    business_date: date
    citizens: list[dict]
    cases: list[dict]
    payments: list[dict]
    case_types: list[dict]


def _cpr(rng: random.Random, used: set[str]) -> tuple[str, str]:
    while True:
        birth = date(1940, 1, 1) + timedelta(days=rng.randrange(0, 365 * 65))
        day = min(birth.day, 28) + 60  # 61..88: impossible calendar day => cannot be a real CPR
        cpr = f"{day:02d}{birth.month:02d}{birth.year % 100:02d}-{rng.randrange(0, 10000):04d}"
        if cpr not in used:
            used.add(cpr)
            return cpr, birth.replace(day=min(birth.day, 28)).isoformat()


def _ts(day: date, rng: random.Random) -> str:
    return datetime(day.year, day.month, day.day, rng.randrange(6, 20), rng.randrange(60), rng.randrange(60)).isoformat() + "Z"


def _new_citizen(rng: random.Random, world: World, day: date) -> Citizen:
    cpr, birth = _cpr(rng, set(world.citizens))
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    postal, city, muni = rng.choice(PLACES)
    ascii_name = (f"{first}.{last}".lower().replace("ø", "oe").replace("æ", "ae").replace("å", "aa"))
    return Citizen(
        cpr=cpr, first_name=first, last_name=last, street=rng.choice(STREETS),
        house_no=str(rng.randrange(1, 180)), postal_code=postal, city=city, municipality_code=muni,
        email=f"{ascii_name}{rng.randrange(10, 99)}@borger.example",
        phone=f"+45 00{rng.randrange(0, 1000000):06d}", birth_date=birth, updated_at=_ts(day, rng),
    )


def _citizen_record(c: Citizen) -> dict:
    if c.deleted:
        return {"cpr": c.cpr, "deleted": True, "updated_at": c.updated_at}
    return {
        "cpr": c.cpr,
        "name": {"first": c.first_name, "last": c.last_name},
        "address": {"street": c.street, "house_no": c.house_no, "postal_code": c.postal_code,
                    "city": c.city, "municipality_code": c.municipality_code},
        "contact": {"email": c.email, "phone": c.phone},
        "birth_date": c.birth_date,
        "deleted": False,
        "updated_at": c.updated_at,
    }


def _new_case(rng: random.Random, world: World, cpr: str, day: date, types: list[tuple]) -> dict:
    world.case_seq += 1
    case = {
        "id": f"S-{day.year}-{world.case_seq:06d}", "cpr": cpr, "type": rng.choice(types)[0],
        "status": "open", "opened": day.isoformat(), "closed": None,
        "caseworker": f"kw{rng.randrange(1, 9):02d}", "updated_at": _ts(day, rng),
    }
    world.cases[case["id"]] = case
    return dict(case)


def _payment(rng: random.Random, world: World, case: dict, pay_day: date, booked_day: date) -> dict:
    world.payment_seq += 1
    amount = rng.choice([-1, 1, 1, 1]) * rng.randrange(5000, 2500000) / 100
    return {
        "payment_id": f"P{world.payment_seq:08d}", "case_id": case["id"], "cpr": case["cpr"],
        # Danish formats on purpose: decimal comma with thousand separator, dd-mm-yyyy
        "amount": f"{amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        "currency": "DKK", "payment_date": pay_day.strftime("%d-%m-%Y"),
        "booked_at": _ts(booked_day, rng).replace("T", " ").rstrip("Z"), "status": "booked",
    }


def _step(world: World, sequence: int, n_citizens: int) -> Batch:
    """Advance the world by one delivery and return what the source system exports."""
    rng = random.Random(f"{world.seed}:{sequence}")  # noqa: S311 - synthetic data, not crypto
    day = BASE_DATE + timedelta(days=sequence - 1)
    types = CASE_TYPES + ([NEW_CASE_TYPE] if sequence >= 2 else [])
    out_citizens: list[dict] = []
    out_cases: list[dict] = []
    out_payments: list[dict] = []

    # master records held back in the previous delivery arrive now (late-arriving dimension members)
    for c in world.citizens.values():
        if not c.delivered:
            c.delivered = True
            c.updated_at = _ts(day, rng)
            out_citizens.append(_citizen_record(c))

    if sequence == 1:
        new_count, held_back = n_citizens, max(2, n_citizens // 50)
    else:
        new_count, held_back = max(3, n_citizens // 20), max(1, n_citizens // 100)
        live = [c for c in world.citizens.values() if c.delivered and not c.deleted]
        for c in rng.sample(live, k=max(2, len(live) // 10)):  # moves / contact changes
            if rng.random() < 0.7:
                c.street, c.house_no = rng.choice(STREETS), str(rng.randrange(1, 180))
                c.postal_code, c.city, c.municipality_code = rng.choice(PLACES)
            else:
                c.email = c.email.replace("@", f".{sequence}@")
            c.updated_at = _ts(day, rng)
            out_citizens.append(_citizen_record(c))
        changed = {r["cpr"] for r in out_citizens}
        for c in rng.sample([c for c in live if c.cpr not in changed], k=max(1, len(live) // 50)):
            c.deleted, c.updated_at = True, _ts(day, rng)  # deletion in the source -> tombstone
            out_citizens.append(_citizen_record(c))

    for i in range(new_count):
        c = _new_citizen(rng, world, day)
        c.delivered = i >= held_back  # first few: facts now, master data next delivery
        world.citizens[c.cpr] = c
        if c.delivered:
            out_citizens.append(_citizen_record(c))

    # cases: updates to existing, then new ones
    open_cases = [c for c in world.cases.values() if c["status"] == "open"]
    for case in rng.sample(open_cases, k=len(open_cases) // 4):
        case["status"] = rng.choice(["closed", "closed", "rejected"])
        case["closed"], case["updated_at"] = day.isoformat(), _ts(day, rng)
        out_cases.append(dict(case))
    active = [c for c in world.citizens.values() if not c.deleted]
    undelivered = [c for c in active if not c.delivered]
    for c in undelivered + rng.sample(active, k=max(5, len(active) // 3)):
        out_cases.append(_new_case(rng, world, c.cpr, day, types))

    # payments: today's, plus late arrivals dated up to 20 days back
    payable = [c for c in world.cases.values() if world.citizens[c["cpr"]].deleted is False]
    for case in rng.sample(payable, k=max(5, len(payable) // 2)):
        late = sequence > 1 and rng.random() < 0.25
        pay_day = day - timedelta(days=rng.randrange(3, 20)) if late else day
        out_payments.append(_payment(rng, world, case, pay_day, day))
    for case_id in [c["id"] for c in out_cases if world.citizens[c["cpr"]].delivered is False]:
        out_payments.append(_payment(rng, world, world.cases[case_id], day, day))

    # in-file duplicates (the same record exported twice)
    if out_citizens:
        out_citizens.append(dict(out_citizens[0]))
    if out_payments:
        out_payments.append(dict(out_payments[-1]))

    case_types = [{"code": code, "name": name, "department": dep, "active": True} for code, name, dep in types]
    return Batch(sequence, day, out_citizens, out_cases, out_payments, case_types)


def simulate(seed: int, sequence: int, citizens: int = 200) -> Batch:
    if sequence < 1:
        raise ValueError("sequence starts at 1")
    world = World(seed=seed)
    batch = None
    for step in range(1, sequence + 1):
        batch = _step(world, step, citizens)
    return batch


# --- native renderings -------------------------------------------------------------------------

def citizens_json(batch: Batch) -> bytes:
    doc = {
        "meta": {"system": "borgerportal", "api_version": "v2", "batch": batch.sequence,
                 "business_date": batch.business_date.isoformat(), "count": len(batch.citizens)},
        "data": batch.citizens,
    }
    return json.dumps(doc, ensure_ascii=False, indent=1).encode("utf-8")


def cases_xml(batch: Batch) -> bytes:
    root = ET.Element("cases", {"batch": str(batch.sequence), "exported": batch.business_date.isoformat()})
    for case in batch.cases:
        elem = ET.SubElement(root, "case", {"id": case["id"]})
        ET.SubElement(elem, "cpr").text = case["cpr"]
        ET.SubElement(elem, "type", {"code": case["type"]})
        ET.SubElement(elem, "status").text = case["status"]
        ET.SubElement(elem, "opened").text = case["opened"]
        ET.SubElement(elem, "closed").text = case["closed"]
        ET.SubElement(elem, "caseworker").text = case["caseworker"]
        ET.SubElement(elem, "updated_at").text = case["updated_at"]
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def payments_csv(batch: Batch) -> bytes:
    buf = io.StringIO(newline="")
    cols = ["payment_id", "case_id", "cpr", "amount", "currency", "payment_date", "booked_at", "status"]
    writer = csv.DictWriter(buf, fieldnames=cols, delimiter=";", lineterminator="\r\n")
    writer.writeheader()
    writer.writerows(batch.payments)
    return buf.getvalue().encode("utf-8")


def case_types_parquet(batch: Batch) -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq

    sink = io.BytesIO()
    table = pa.Table.from_pylist(batch.case_types)
    # fixed writer settings keep the bytes reproducible
    pq.write_table(table, sink, compression="snappy", write_statistics=False, store_schema=False)
    return sink.getvalue()
