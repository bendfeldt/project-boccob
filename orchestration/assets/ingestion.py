"""Ingestion assets, generated from the source specs: adding a source adds its assets.

Per source two multi-assets (one dlt run each, one output per entity):

    landing/<source>/<entity>   native files in the raw bucket      (stage LAND)
    raw/<source>/<entity>       schema-on-read Iceberg index table  (stage INDEX)

The raw asset keys equal the keys of the dbt sources, which connects the lineage to dbt.
"""

import os

import dagster as dg

from ingestion.framework.spec import SourceSpec, list_sources
from orchestration.resources import IngestionResource

WRITER_POOL = "lakehouse_writer"
TIMEZONE = "Europe/Copenhagen"  # partitions and schedules
daily_partitions = dg.DailyPartitionsDefinition(
    start_date=os.environ.get("INGEST_START_DATE", "2026-01-01"), timezone=TIMEZONE
)


def landing_key(source: str, entity: str) -> dg.AssetKey:
    return dg.AssetKey(["landing", source, entity])


def raw_key(source: str, entity: str) -> dg.AssetKey:
    return dg.AssetKey(["raw", source, entity])


def build_source_assets(source: SourceSpec) -> list[dg.AssetsDefinition]:
    has_pii = any(e.identifiers for e in source.entities)
    common_tags = {"layer": "raw", "source": source.name, **({"pii": "true"} if has_pii else {})}

    @dg.multi_asset(
        name=f"land_{source.name}",
        group_name=f"raw_{source.name}",
        partitions_def=daily_partitions,
        pool=WRITER_POOL,
        specs=[
            dg.AssetSpec(
                key=landing_key(source.name, e.name),
                description=f"Native {e.format} files of {source.name}.{e.name} in the raw bucket. {e.description}",
                kinds={"dlt", "s3", e.format},
                owners=[f"team:{source.owner}"] if source.owner else [],
                tags=common_tags,
            )
            for e in source.entities
        ],
    )
    def land(context: dg.AssetExecutionContext, ingestion: IngestionResource):
        # one result per declared entity, also when nothing came: a skipped output would skip
        # indexing and dbt for every source and still report the run as successful
        result = ingestion.land(source.name, logical_date=context.partition_key)
        delivered = result.summary()["entities"] if result.committed else {}
        if not delivered:
            context.log.info("source delivered nothing for %s", context.partition_key)
        for e in source.entities:
            stats = delivered.get(e.name, {"files": 0, "bytes": 0, "records": 0})
            yield dg.MaterializeResult(
                asset_key=landing_key(source.name, e.name),
                metadata={"load_id": result.load_id or "", **stats},
            )

    @dg.multi_asset(
        name=f"index_{source.name}",
        group_name=f"raw_{source.name}",
        partitions_def=daily_partitions,
        pool=WRITER_POOL,
        specs=[
            dg.AssetSpec(
                key=raw_key(source.name, e.name),
                deps=[landing_key(source.name, e.name)],
                description=f"Schema-on-read index raw.{source.name}.{e.name} (Iceberg via Polaris)",
                kinds={"dlt", "iceberg"},
                owners=[f"team:{source.owner}"] if source.owner else [],
                tags=common_tags,
            )
            for e in source.entities
        ],
    )
    def index(context: dg.AssetExecutionContext, ingestion: IngestionResource):
        result = ingestion.index(source.name)  # all committed loads that are not indexed yet
        for e in source.entities:
            yield dg.MaterializeResult(
                asset_key=raw_key(source.name, e.name),
                metadata={"rows_indexed": result.rows.get(e.name, 0), "load_ids": ", ".join(result.load_ids)},
            )

    return [land, index]


SOURCES = list_sources()
ingestion_assets = [asset for source in SOURCES for asset in build_source_assets(source)]
