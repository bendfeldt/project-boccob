"""dbt assets (clean, consolidated, curated) with lineage back to the raw index assets.

dbt data tests become Dagster asset checks; dbt meta (owner, layer, PII) becomes asset metadata.
"""

from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DagsterDbtTranslatorSettings, DbtCliResource, dbt_assets

from orchestration.resources import dbt_project

WRITER_POOL = "lakehouse_writer"


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
