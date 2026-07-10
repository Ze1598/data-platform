from typing import Any

import trino
from dagster import ConfigurableResource


class TrinoResource(ConfigurableResource):
    """Executes SQL against Trino. Used by the stub extraction assets to
    write into the `clean` Iceberg schema — the real dbt-owned merge logic
    (clean -> staging -> model -> serve) runs via dbt_assets, not this."""

    host: str
    port: int
    user: str = "dagster"
    catalog: str = "iceberg"

    def execute(self, sql: str, schema: str | None = None) -> list[tuple[Any, ...]]:
        conn = trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=schema,
        )
        try:
            cur = conn.cursor()
            cur.execute(sql)
            return cur.fetchall()
        finally:
            conn.close()
