"""The code location must load without any platform service (needs a dbt manifest: `make dbt-parse`)."""

import os
from pathlib import Path

import pytest

MANIFEST = Path(os.environ.get("DBT_PROJECT_DIR", Path(__file__).parents[2] / "transform")) / "target" / "manifest.json"
pytestmark = pytest.mark.skipif(not MANIFEST.is_file(), reason="dbt manifest missing - run `make dbt-parse`")


@pytest.fixture(scope="module")
def defs():
    from orchestration.definitions import defs

    return defs


def test_definitions_resolve(defs):
    import dagster as dg

    dg.Definitions.validate_loadable(defs)
    names = {j.name for j in defs.resolve_all_job_defs()}
    assert {"ingest_borgerportal", "transform", "daily_refresh", "iceberg_maintenance", "retention", "erasure"} <= names
    assert {s.name for s in defs.schedules} >= {
        "daily_refresh_schedule",
        "erasure_schedule",
        "retention_schedule",
        "iceberg_maintenance_schedule",
    }


def test_lineage_spans_all_layers(defs):
    import dagster as dg

    graph = defs.resolve_asset_graph()
    key = dg.AssetKey

    def parents(k):
        return graph.get(k).parent_keys

    assert key(["landing", "borgerportal", "citizens"]) in parents(key(["raw", "borgerportal", "citizens"]))
    assert key(["raw", "borgerportal", "citizens"]) in parents(key(["clean", "borgerportal", "citizens"]))
    assert key(["clean", "borgerportal", "citizens"]) in parents(key(["consolidated", "party", "person"]))
    assert key(["consolidated", "party", "person"]) in parents(key(["curated", "borgerservice", "dim_citizen"]))
    # the consolidated layer is optional: this dimension reads clean directly
    assert key(["clean", "borgerportal", "case_types"]) in parents(key(["curated", "borgerservice", "dim_case_type"]))
    fact_parents = parents(key(["curated", "borgerservice", "fact_payment"]))
    assert key(["curated", "borgerservice", "dim_citizen"]) in fact_parents


def test_dbt_tests_become_asset_checks(defs):
    graph = defs.resolve_asset_graph()
    checks = {c.asset_key.to_user_string() for c in graph.asset_check_keys}
    assert "curated/borgerservice/dim_citizen" in checks
    assert len(graph.asset_check_keys) >= 40


def test_erasure_job_takes_no_identifier(defs):
    """Identifiers must never appear in run config (it is stored and shown by the orchestrator)."""
    import inspect

    from orchestration.jobs import erasure_op
    from orchestration.resources import GovernanceResource

    assert list(inspect.signature(GovernanceResource.process_erasure_requests).parameters) == ["self"]
    assert set(inspect.signature(erasure_op.compute_fn.decorated_fn).parameters) == {"context", "governance"}
    assert defs.resolve_job_def("erasure").partitions_def is None


def test_daily_refresh_is_partitioned(defs):
    assert defs.resolve_job_def("daily_refresh").partitions_def is not None
    assert defs.resolve_job_def("transform").partitions_def is None
