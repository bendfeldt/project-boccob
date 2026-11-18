"""Dagster resources. They hold no connection details themselves: everything is resolved from
environment variables at execution time (see docs/platform-contract.md), so the code location
loads without any service being reachable."""

import os
from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject

REPO = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", REPO / "transform"))
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR))

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR)
# dev convenience: (re)build the manifest when running `dagster dev`; images ship a prepared one
dbt_project.prepare_if_dev()


class IngestionResource(dg.ConfigurableResource):
    """Runs the two ingestion stages of a source."""

    def land(self, source_name: str, logical_date: str | None = None):
        from ingestion.framework.landing import land_source
        from ingestion.framework.spec import load_source

        return land_source(load_source(source_name), logical_date=logical_date)

    def index(self, source_name: str, load_id: str | None = None):
        from ingestion.framework.indexing import index_source
        from ingestion.framework.spec import load_source

        return index_source(load_source(source_name), load_id=load_id)


class GovernanceResource(dg.ConfigurableResource):
    """Maintenance, retention and erasure tooling."""

    verify_storage_after_erasure: str = "off"  # off | scoped | full (ERASURE_VERIFY_STORAGE)

    def migrate(self, dry_run: bool = False, check_drift: bool = True):
        from governance.migrations.runner import run_migrations

        return run_migrations(dry_run=dry_run, check_drift=check_drift)

    def maintain(self, layers: tuple[str, ...], operations: tuple[str, ...]):
        from governance.maintenance.runner import run_maintenance

        return run_maintenance(layers=layers, operations=operations)

    def apply_retention(self, dry_run: bool = False):
        from governance.retention.runner import run_retention

        return run_retention(dry_run=dry_run)

    def process_erasure_requests(self):
        from governance.erasure.executor import process_pending_requests

        return process_pending_requests(verify_storage=self.verify_storage_after_erasure)


RESOURCES = {
    "ingestion": IngestionResource(),
    "governance": GovernanceResource(
        verify_storage_after_erasure=os.environ.get("ERASURE_VERIFY_STORAGE", "false"),
    ),
    "dbt": DbtCliResource(project_dir=dbt_project),
}
