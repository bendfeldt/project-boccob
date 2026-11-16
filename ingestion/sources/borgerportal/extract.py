"""Extractor for the synthetic Borgerportal source.

A real source would call its API / read its export share here, using credentials from the
environment (``dlt.secrets``), and keep its cursor in ``ctx.state``. The contract is the same:
yield the delivered files unmodified as ``NativeFile`` objects.
"""

from __future__ import annotations

from collections.abc import Iterator

from ingestion.framework.spec import ExtractContext, NativeFile
from ingestion.sources.borgerportal import generator


def extract(ctx: ExtractContext) -> Iterator[NativeFile]:
    sequence = int(ctx.state.get("sequence", 0)) + 1
    batch = generator.simulate(int(ctx.options["seed"]), sequence, int(ctx.options["citizens"]))
    tag = f"{batch.business_date:%Y%m%d}_{sequence:04d}"
    yield NativeFile("citizens", f"citizens_{tag}.json", generator.citizens_json(batch), len(batch.citizens))
    yield NativeFile("cases", f"cases_{tag}.xml", generator.cases_xml(batch), len(batch.cases))
    yield NativeFile("payments", f"payments_{tag}.csv", generator.payments_csv(batch), len(batch.payments))
    yield NativeFile("case_types", f"case_types_{tag}.parquet", generator.case_types_parquet(batch),
                     len(batch.case_types))
    ctx.state["sequence"] = sequence  # committed by dlt only if the whole load succeeds
