"""Dagster code location of the Boccob lakehouse solution."""

import dagster as dg

from orchestration.assets.ingestion import ingestion_assets
from orchestration.assets.transform import lakehouse_dbt_assets, schema_migrations
from orchestration.jobs import JOBS, SCHEDULES
from orchestration.resources import RESOURCES

defs = dg.Definitions(
    assets=[*ingestion_assets, schema_migrations, lakehouse_dbt_assets],
    jobs=JOBS,
    schedules=SCHEDULES,
    resources=RESOURCES,
)
