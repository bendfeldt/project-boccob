"""Jobs and schedules: daily refresh, Iceberg maintenance, retention, right to erasure."""

import os

import dagster as dg

from lakehouse.settings import LAYERS
from orchestration.assets.ingestion import SOURCES, TIMEZONE
from orchestration.assets.transform import MIGRATIONS_KEY
from orchestration.resources import GovernanceResource

WRITER_POOL = "lakehouse_writer"
LAYER_GROUPS = (
    dg.AssetSelection.tag("layer", "clean")
    | dg.AssetSelection.tag("layer", "consolidated")
    | dg.AssetSelection.tag("layer", "curated")
)

# Jobs that remove object versions (erasure, retention, maintenance) need the PURGE_S3_* credentials.
# With PURGE_CREDENTIALS_SECRET set (name of a Kubernetes Secret holding them), only the run pods of
# these jobs get that Secret (K8sRunLauncher per-job config), so the platform can drop the purge
# credential from the environment shared by every run (audit N-3).
_PURGE_SECRET = os.environ.get("PURGE_CREDENTIALS_SECRET", "").strip()
PURGE_TAGS = (
    {"dagster-k8s/config": {"container_config": {"env_from": [{"secret_ref": {"name": _PURGE_SECRET}}]}}}
    if _PURGE_SECRET
    else {}
)

# --- asset jobs ------------------------------------------------------------------------------

ingest_jobs = [
    dg.define_asset_job(
        name=f"ingest_{source.name}",
        description=f"Land and index one delivery of {source.name}",
        selection=dg.AssetSelection.groups(f"raw_{source.name}"),
    )
    for source in SOURCES
]

migrate_job = dg.define_asset_job(
    name="migrate",
    description="Apply pending schema migrations (transform/migrations); first step of a release, docs/releases.md",
    selection=dg.AssetSelection.assets(MIGRATIONS_KEY),
)

transform_job = dg.define_asset_job(
    name="transform",
    description="Schema migrations, then dbt build of clean, consolidated and curated (incl. keystore and tests)",
    selection=LAYER_GROUPS | dg.AssetSelection.assets(MIGRATIONS_KEY),
)

# --- governance ops --------------------------------------------------------------------------


class MaintenanceConfig(dg.Config):
    layers: list[str] = list(LAYERS)  # noqa: RUF012 - pydantic copies defaults
    operations: list[str] = ["optimize", "expire_snapshots", "remove_orphan_files", "purge_noncurrent_versions"]  # noqa: RUF012


@dg.op(pool=WRITER_POOL, description="optimize / expire_snapshots / remove_orphan_files on all layer tables")
def iceberg_maintenance_op(context: dg.OpExecutionContext, config: MaintenanceConfig, governance: GovernanceResource):
    report = governance.maintain(tuple(config.layers), tuple(config.operations))
    context.add_output_metadata({"operations_ok": len(report.done), "operations_failed": len(report.failed)})
    if not report.ok:
        raise dg.Failure(description="maintenance failed for some tables", metadata={"failed": report.failed})
    return len(report.done)


@dg.op(
    pool=WRITER_POOL,
    out=dg.Out(dg.Nothing),
    description="Apply pending schema migrations first: governance SQL is derived from this release's models",
)
def migrate_op(context: dg.OpExecutionContext, governance: GovernanceResource):
    # no drift check here: an unrelated drift must not block erasure or retention (transform checks it)
    report = governance.migrate(check_drift=False, holder=f"dagster run {context.run_id}")
    context.log.info("migrations applied: %s", report.applied)


class RetentionConfig(dg.Config):
    dry_run: bool = False


@dg.op(
    pool=WRITER_POOL,
    ins={"after_migrations": dg.In(dg.Nothing)},
    description="Apply the retention policy to raw files, raw index and governed tables",
)
def retention_op(context: dg.OpExecutionContext, config: RetentionConfig, governance: GovernanceResource):
    report = governance.apply_retention(dry_run=config.dry_run)
    context.add_output_metadata(
        {"tables": report["tables"], "landing_objects": report["landing_objects"], "dry_run": report["dry_run"]}
    )
    return report["landing_objects"]


@dg.op(
    pool=WRITER_POOL,
    ins={"after_migrations": dg.In(dg.Nothing)},
    description="Execute queued right-to-erasure requests (no identifiers in run config or logs)",
)
def erasure_op(context: dg.OpExecutionContext, governance: GovernanceResource):
    from governance.erasure.executor import ErasureBatchError

    try:
        reports = governance.process_erasure_requests()
    except ErasureBatchError as exc:
        # every request was attempted; the failed ones stay queued (status 'failed') for the next run
        failed = [r for r in exc.reports if r.status != "completed"]
        raise dg.Failure(
            description=f"{len(failed)} of {len(exc.reports)} erasure request(s) failed and stay queued",
            metadata={
                "requests_processed": len(exc.reports),
                "failed_request_ids": [r.request_id for r in failed],
                "errors": [r.error or "" for r in failed],
                "completed_erasure_ids": [r.erasure_id for r in exc.reports if r.status == "completed"],
            },
        ) from exc
    context.add_output_metadata(
        {
            "requests_processed": len(reports),
            "erasure_ids": [r.erasure_id for r in reports],
            "tables_purged": sorted({t for r in reports for t in r.purged_tables}),
        }
    )
    return len(reports)


@dg.job(description="Iceberg housekeeping for all layers", tags={"lakehouse/kind": "maintenance", **PURGE_TAGS})
def iceberg_maintenance():
    iceberg_maintenance_op()


@dg.job(
    description="Time based retention (governance/retention.yml + dbt meta.gdpr)",
    tags={"lakehouse/kind": "retention", **PURGE_TAGS},
)
def retention():
    retention_op(after_migrations=migrate_op())


@dg.job(
    description="GDPR art. 17: erase queued data subjects from all layers and purge history",
    tags={"lakehouse/kind": "erasure", **PURGE_TAGS},
)
def erasure():
    erasure_op(after_migrations=migrate_op())


# --- schedules -------------------------------------------------------------------------------
# All schedules ship stopped unless SCHEDULES_ENABLED=true (the platform enables them per environment).
_status = (
    dg.DefaultScheduleStatus.RUNNING
    if os.environ.get("SCHEDULES_ENABLED", "false").lower() in ("1", "true")
    else dg.DefaultScheduleStatus.STOPPED
)

# one schedule per source: a failing source does not hold back the others; the dbt build runs
# on its own afterwards with whatever was delivered
ingest_schedules = [
    dg.build_schedule_from_partitioned_job(
        job,
        hour_of_day=2,
        minute_of_hour=15,
        default_status=_status,
        description=f"Nightly load of yesterday's delivery ({job.name})",
    )
    for job in ingest_jobs
]
transform_schedule = dg.ScheduleDefinition(
    name="transform_schedule",
    job=transform_job,
    cron_schedule="0 4 * * *",
    execution_timezone=TIMEZONE,
    default_status=_status,
    description="Schema migrations and dbt build after the nightly ingestion",
)
erasure_schedule = dg.ScheduleDefinition(
    name="erasure_schedule",
    job=erasure,
    cron_schedule="30 0 * * *",
    execution_timezone=TIMEZONE,
    default_status=_status,
    description="Process queued erasure requests every night before the refresh",
)
retention_schedule = dg.ScheduleDefinition(
    name="retention_schedule",
    job=retention,
    cron_schedule="0 1 * * *",
    execution_timezone=TIMEZONE,
    default_status=_status,
)
maintenance_schedule = dg.ScheduleDefinition(
    name="iceberg_maintenance_schedule",
    job=iceberg_maintenance,
    cron_schedule="0 5 * * 0",
    execution_timezone=TIMEZONE,
    default_status=_status,
)

JOBS = [*ingest_jobs, migrate_job, transform_job, iceberg_maintenance, retention, erasure]
SCHEDULES = [*ingest_schedules, transform_schedule, erasure_schedule, retention_schedule, maintenance_schedule]
