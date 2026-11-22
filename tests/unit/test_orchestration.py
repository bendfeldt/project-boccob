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
    assert {"ingest_borgerportal", "transform", "iceberg_maintenance", "retention", "erasure"} <= names
    assert {s.name for s in defs.schedules} >= {
        "ingest_borgerportal_schedule",
        "transform_schedule",
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


def test_ingestion_is_partitioned_and_separate_from_dbt(defs):
    """One failing source must not stop the dbt build or the other sources."""
    ingest = defs.resolve_job_def("ingest_borgerportal")
    assert ingest.partitions_def is not None
    assert all(k.path[0] in ("landing", "raw") for k in ingest.asset_layer.selected_asset_keys)
    assert defs.resolve_job_def("transform").partitions_def is None


def test_migrations_run_before_every_dbt_model(defs):
    """The schema migrations are part of the transform job and precede every model."""
    import dagster as dg

    from orchestration.assets.transform import MIGRATIONS_KEY

    graph = defs.resolve_asset_graph()
    models = [k for k in graph.get_all_asset_keys() if k.path[0] in ("clean", "consolidated", "curated")]
    models = [k for k in models if k != MIGRATIONS_KEY]
    assert models

    def ancestors(k):
        seen, todo = set(), [k]
        while todo:
            for parent in graph.get(todo.pop()).parent_keys:
                if parent not in seen:
                    seen.add(parent)
                    todo.append(parent)
        return seen

    for k in models:
        assert MIGRATIONS_KEY in ancestors(k), k
    assert MIGRATIONS_KEY in graph.get(dg.AssetKey(["clean", "borgerportal", "citizens"])).parent_keys
    for job in ("migrate", "transform"):
        assert MIGRATIONS_KEY in defs.resolve_job_def(job).asset_layer.selected_asset_keys, job


def test_purge_tag_sets_only_env_from(monkeypatch):
    """the platform restricts user-defined k8s config to container_config.env_from
    (only_allow_user_defined_k8s_config_fields); the solution's only k8s tag must fit that."""
    import importlib

    import orchestration.jobs as jobs

    monkeypatch.setenv("PURGE_CREDENTIALS_SECRET", "lakehouse-purge-env")
    reloaded = importlib.reload(jobs)
    try:
        import json

        from dagster_k8s.container_context import K8sContainerContext
        from dagster_k8s.job import UserDefinedDagsterK8sConfig

        shape = {"container_config": {"env_from": [{"secret_ref": {"name": "lakehouse-purge-env"}}]}}
        assert {"dagster-k8s/config": shape} == reloaded.PURGE_TAGS
        allow = {"container_config": {"env_from": True}}  # the platform's allowlist
        for job in (reloaded.erasure, reloaded.retention, reloaded.iceberg_maintenance):
            tag = json.loads(job.tags["dagster-k8s/config"])
            assert tag == shape
            ctx = K8sContainerContext(run_k8s_config=UserDefinedDagsterK8sConfig.from_dict(tag))
            ctx.validate_user_k8s_config_for_run(allow, None)  # raises on any other field
        forged = {**shape, "pod_template_spec_metadata": {"labels": {"dagster/job": "erasure"}}}
        with pytest.raises(Exception, match="violated the allowed list"):
            K8sContainerContext(
                run_k8s_config=UserDefinedDagsterK8sConfig.from_dict(forged)
            ).validate_user_k8s_config_for_run(allow, None)
        for job in [*reloaded.ingest_jobs, reloaded.migrate_job, reloaded.transform_job]:
            assert "dagster-k8s/config" not in (job.tags or {}), job.name
    finally:
        monkeypatch.delenv("PURGE_CREDENTIALS_SECRET")
        importlib.reload(jobs)


@pytest.mark.parametrize("delivered", [[], ["citizens"]])
def test_partial_or_empty_delivery_still_indexes(delivered):
    """A day without a file for some (or all) entities must not skip the downstream steps."""
    import dagster as dg

    from ingestion.framework.indexing import IndexResult
    from ingestion.framework.landing import LandResult
    from ingestion.framework.manifest import ManifestEntry
    from orchestration.assets.ingestion import SOURCES, build_source_assets
    from orchestration.resources import IngestionResource

    source = next(s for s in SOURCES if s.name == "borgerportal")
    calls = []

    class Fake(IngestionResource):
        def land(self, source_name, logical_date=None):
            files = [ManifestEntry(source_name, e, "1.5", "d", "d", "f", "p", "j", 1, "s", 1, "t") for e in delivered]
            return LandResult(source_name, "1.5" if files else None, files)

        def index(self, source_name, load_id=None):
            calls.append(source_name)
            return IndexResult(source_name)

    result = dg.materialize(build_source_assets(source), resources={"ingestion": Fake()}, partition_key="2026-11-01")
    assert result.success and calls == ["borgerportal"], "index ran"
    materialized = {e.asset_key.path[-1] for e in result.get_asset_materialization_events() if e.asset_key}
    assert {e.name for e in source.entities} <= materialized
