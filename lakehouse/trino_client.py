"""Thin Trino DB-API wrapper used by governance tooling and tests."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Sequence
from typing import Any

import trino

from lakehouse.settings import TrinoSettings

log = logging.getLogger(__name__)
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def ident(name: str) -> str:
    """Validate and quote an SQL identifier (defence against injection through config values)."""
    if not _IDENT.match(name):
        raise ValueError(f"illegal SQL identifier: {name!r}")
    return f'"{name}"'


def fqn(catalog: str, schema: str, table: str) -> str:
    return f"{ident(catalog)}.{ident(schema)}.{ident(table)}"


class TrinoClient:
    def __init__(self, settings: TrinoSettings | None = None, *, user: str | None = None,
                 session_properties: dict[str, str] | None = None, source: str = "boccob-solution"):
        self.settings = settings or TrinoSettings.from_env()
        auth = None
        if self.settings.jwt_token:
            auth = trino.auth.JWTAuthentication(self.settings.jwt_token)
        elif self.settings.password:
            auth = trino.auth.BasicAuthentication(user or self.settings.user, self.settings.password)
        self._conn = trino.dbapi.connect(
            host=self.settings.host,
            port=self.settings.port,
            user=user or self.settings.user,
            http_scheme=self.settings.http_scheme,
            auth=auth,
            verify=self.settings.verify,
            source=source,
            session_properties=session_properties or {},
        )

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> list[list[Any]]:
        log.debug("trino: %s", sql)
        cur = self._conn.cursor()
        cur.execute(sql, params) if params else cur.execute(sql)
        return cur.fetchall()

    def scalar(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        rows = self.execute(sql, params)
        return rows[0][0] if rows else None

    def execute_many(self, statements: Iterable[str]) -> None:
        for stmt in statements:
            self.execute(stmt)

    def table_exists(self, catalog: str, schema: str, table: str) -> bool:
        rows = self.execute(
            f"select 1 from {ident(catalog)}.information_schema.tables where table_schema = ? and table_name = ?",
            [schema, table],
        )
        return bool(rows)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> TrinoClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
