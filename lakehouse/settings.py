"""Runtime settings, resolved only from environment variables.

In the cluster the variables are injected from Kubernetes Secrets that the External Secrets
Operator materialises from OpenBao. Nothing here has a secret default; non-secret values have
defaults that follow the architecture contract (bucket and catalog names).
The authoritative list is documented in docs/platform-contract.md.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

LAYERS = ("raw", "clean", "consolidated", "curated")


class MissingSettingError(RuntimeError):
    pass


def env(name: str, default: str | None = None, *, required: bool = False) -> str | None:
    """Read a setting. ``NAME_FILE`` (path to a mounted secret file) wins over ``NAME``."""
    file_var = os.environ.get(f"{name}_FILE")
    if file_var:
        return Path(file_var).read_text(encoding="utf-8").strip()
    value = os.environ.get(name, default)
    if required and not value:
        raise MissingSettingError(f"required environment variable {name} (or {name}_FILE) is not set")
    return value


def env_bool(name: str, default: bool = False) -> bool:
    value = env(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class S3Settings:
    endpoint_url: str | None
    access_key_id: str
    secret_access_key: str = field(repr=False)
    region: str
    raw_bucket: str
    index_prefix: str
    ca_bundle: str | None = None

    @classmethod
    def from_env(cls) -> S3Settings:
        return cls(
            endpoint_url=env("S3_ENDPOINT_URL"),
            access_key_id=env("S3_ACCESS_KEY_ID", required=True),
            secret_access_key=env("S3_SECRET_ACCESS_KEY", required=True),
            region=env("S3_REGION", "us-east-1"),
            raw_bucket=env("RAW_BUCKET", "lakehouse-raw"),
            index_prefix=env("RAW_INDEX_PREFIX", "iceberg").strip("/"),
            ca_bundle=env("LAKEHOUSE_CA_BUNDLE"),
        )

    @property
    def raw_bucket_url(self) -> str:
        return f"s3://{self.raw_bucket}"

    @property
    def raw_index_url(self) -> str:
        return f"s3://{self.raw_bucket}/{self.index_prefix}"

    def filesystem(self):
        """fsspec filesystem for the object store (credentials passed explicitly, never ambient)."""
        import s3fs

        client_kwargs: dict = {"region_name": self.region}
        if self.endpoint_url:
            client_kwargs["endpoint_url"] = self.endpoint_url
        if self.ca_bundle:
            client_kwargs["verify"] = self.ca_bundle
        return s3fs.S3FileSystem(
            key=self.access_key_id,
            secret=self.secret_access_key,
            client_kwargs=client_kwargs,
            config_kwargs={"s3": {"addressing_style": "path"}},
            skip_instance_cache=True,
        )


@dataclass(frozen=True)
class PolarisSettings:
    uri: str
    client_id: str
    client_secret: str = field(repr=False)
    scope: str = "PRINCIPAL_ROLE:ALL"
    oauth2_server_uri: str | None = None
    raw_catalog: str = "raw"

    @classmethod
    def from_env(cls) -> PolarisSettings:
        return cls(
            uri=env("POLARIS_URI", required=True),
            client_id=env("POLARIS_CLIENT_ID", required=True),
            client_secret=env("POLARIS_CLIENT_SECRET", required=True),
            scope=env("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL"),
            oauth2_server_uri=env("POLARIS_OAUTH2_SERVER_URI"),
            raw_catalog=env("RAW_CATALOG", "raw"),
        )

    def catalog_properties(self, warehouse: str, s3: S3Settings) -> dict[str, str]:
        """pyiceberg REST catalog properties for one Polaris catalog (= warehouse)."""
        props = {
            "type": "rest",
            "uri": self.uri,
            "warehouse": warehouse,
            "credential": f"{self.client_id}:{self.client_secret}",
            "scope": self.scope,
            # static object-store credentials; no credential vending (works without STS)
            "header.X-Iceberg-Access-Delegation": "",
            "s3.access-key-id": s3.access_key_id,
            "s3.secret-access-key": s3.secret_access_key,
            "s3.region": s3.region,
            "s3.force-virtual-addressing": "false",
        }
        if s3.endpoint_url:
            props["s3.endpoint"] = s3.endpoint_url
        if self.oauth2_server_uri:
            props["oauth2-server-uri"] = self.oauth2_server_uri
        if s3.ca_bundle:
            props["ssl"] = {"cabundle": s3.ca_bundle}  # type: ignore[assignment]
        return props


@dataclass(frozen=True)
class TrinoSettings:
    host: str
    port: int
    user: str
    http_scheme: str = "https"
    password: str | None = field(default=None, repr=False)
    jwt_token: str | None = field(default=None, repr=False)
    verify: str | bool = True

    @classmethod
    def from_env(cls) -> TrinoSettings:
        scheme = env("TRINO_HTTP_SCHEME", "https")
        ca = env("TRINO_CA_BUNDLE") or env("LAKEHOUSE_CA_BUNDLE")
        return cls(
            host=env("TRINO_HOST", required=True),
            port=int(env("TRINO_PORT", "8443" if scheme == "https" else "8080")),
            user=env("TRINO_USER", required=True),
            http_scheme=scheme,
            password=env("TRINO_PASSWORD"),
            jwt_token=env("TRINO_JWT_TOKEN"),
            verify=ca if ca else True,
        )


def pseudonym_pepper() -> str:
    """Secret pepper for keyed pseudonymisation (HMAC-SHA256). Runtime only, never in git."""
    pepper = env("PSEUDONYM_PEPPER", required=True)
    if len(pepper) < 32:
        raise MissingSettingError("PSEUDONYM_PEPPER must be at least 32 characters")
    return pepper
