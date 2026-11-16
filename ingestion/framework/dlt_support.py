"""dlt wiring shared by both stages: destinations and pipelines built from env-only settings."""

from __future__ import annotations

import os
import tempfile
from typing import Any

import dlt
from dlt.common.configuration.specs import AwsCredentials
from dlt.destinations import filesystem

from lakehouse.settings import PolarisSettings, S3Settings


def aws_credentials(s3: S3Settings) -> AwsCredentials:
    creds = AwsCredentials()
    creds.aws_access_key_id = s3.access_key_id
    creds.aws_secret_access_key = s3.secret_access_key
    creds.region_name = s3.region
    creds.endpoint_url = s3.endpoint_url
    return creds


def pipelines_dir() -> str:
    """Working directory for dlt. Pods are stateless: state is restored from the destination."""
    return os.environ.get("DLT_PIPELINES_DIR") or tempfile.mkdtemp(prefix="dlt-pipelines-")


def configure_runtime() -> None:
    """Process-wide dlt behaviour that must not depend on a config file being present."""
    # manifests / state files stay plain text so auditors can read them with any tool
    os.environ.setdefault("DATA_WRITER__DISABLE_COMPRESSION", "true")
    os.environ.setdefault("RUNTIME__DLTHUB_TELEMETRY", "false")


def landing_destination(s3: S3Settings) -> Any:
    return filesystem(
        bucket_url=s3.raw_bucket_url,
        credentials=aws_credentials(s3),
        layout="{table_name}/{load_id}.{file_id}.{ext}",
        destination_name="raw_landing",
    )


def index_destination(s3: S3Settings, polaris: PolarisSettings) -> Any:
    """filesystem destination in Iceberg table format, registered through the Polaris REST catalog."""
    dlt.secrets["iceberg_catalog.iceberg_catalog_name"] = polaris.raw_catalog
    dlt.secrets["iceberg_catalog.iceberg_catalog_type"] = "rest"
    dlt.secrets["iceberg_catalog.iceberg_catalog_config"] = polaris.catalog_properties(polaris.raw_catalog, s3)
    return filesystem(
        bucket_url=s3.raw_index_url,
        credentials=aws_credentials(s3),
        destination_name="raw_index",
    )
