"""dbt assets (clean, consolidated, curated) with lineage back to the raw index assets.

dbt data tests become Dagster asset checks; dbt meta (owner, layer, PII) becomes asset metadata.
"""

from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DagsterDbtTranslatorSettings, DbtCliResource, dbt_assets

from orchestration.resources import GovernanceResource, dbt_project

WRITER_POOL = "lakehouse_writer"
MIGRATIONS_KEY = dg.AssetKey(["curated", "audit", "schema_migrations"])


@dg.asset(
    key=MIGRATIONS_KEY,
    group_name="governance",
    pool=WRITER_POOL,
    kinds={"trino", "iceberg"},
    description=(
        "Versioned schema migrations (transform/migrations): bring existing tables to the shape the "
        "release's models expect before any dbt model runs; record them in curated.audit.schema_migrations"
    ),
)
def schema_migrations(context: dg.AssetExecutionContext, governance: GovernanceResource) -> dg.MaterializeResult:
    report = governance.migrate(holder=f"dagster run {context.run_id}")
    context.log.info("migrations applied: %s; already applied: %d", report.applied, len(report.already_applied))
    return dg.MaterializeResult(
        metadata={
            "applied": report.applied,
            "already_applied": len(report.already_applied),
            "history_purged": report.purged,
        }
    )


def _meta(props: Mapping[str, Any]) -> dict[str, Any]:
    return {**(props.get("meta") or {}), **((props.get("config") or {}).get("meta") or {})}


class LakehouseDbtTranslator(DagsterDbtTranslator):
    """Asset keys follow the lakehouse addressing: <layer catalog>/<schema>/<table>."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            # dbt source "raw_<source>" -> raw/<source>/<entity>: same key as the index assets
            return dg.AssetKey(["raw", dbt_resource_props["schema"], dbt_resource_props["name"]])
        layer = _meta(dbt_resource_props).get("layer") or dbt_resource_props["database"]
        return dg.AssetKey([layer, dbt_resource_props["schema"], dbt_resource_props["name"]])

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        layer = _meta(dbt_resource_props).get("layer", "dbt")
        return f"{layer}_{dbt_resource_props['schema']}"

    def get_asset_spec(self, manifest: Mapping[str, Any], unique_id: str, project: Any) -> dg.AssetSpec:
        """Root models (no upstream model) wait for the schema migrations: a release's migrations
        always run before its first dbt model, in every job that selects both."""
        spec = super().get_asset_spec(manifest, unique_id, project)
        node = manifest.get("nodes", {}).get(unique_id) or {}
        if node.get("resource_type") == "model":
            upstream = (node.get("depends_on") or {}).get("nodes") or []
            if not any(u.startswith("model.") for u in upstream):
                spec = spec.merge_attributes(deps=[MIGRATIONS_KEY])
        return spec

    def get_tags(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, str]:
        tags = dict(super().get_tags(dbt_resource_props))
        meta = _meta(dbt_resource_props)
        if meta.get("layer"):
            tags["layer"] = meta["layer"]
        levels = {(_meta(c).get("pii") or "none") for c in (dbt_resource_props.get("columns") or {}).values()}
        if levels & {"direct", "sensitive"}:
            tags["pii"] = "true"
        return tags


translator = LakehouseDbtTranslator(settings=DagsterDbtTranslatorSettings(enable_asset_checks=True))


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=translator, pool=WRITER_POOL)
def lakehouse_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
