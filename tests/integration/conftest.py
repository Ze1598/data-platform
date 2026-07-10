import os

import psycopg
import pytest
import trino.dbapi


@pytest.fixture(scope="session")
def trino_conn():
    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="integration_tests",
        catalog="iceberg",
    )
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def metadata_conn():
    conn = psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "platform"),
        password=os.environ.get("POSTGRES_PASSWORD", "platform"),
        dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
    )
    yield conn
    conn.close()


def describe_columns(trino_conn, schema: str, table: str) -> dict[str, str]:
    """{column_name: trino_type_string} for schema.table, or {} if the
    table doesn't exist yet (callers decide whether that's a failure)."""
    cur = trino_conn.cursor()
    try:
        cur.execute(f"DESCRIBE iceberg.{schema}.{table}")
        return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as e:
        if "does not exist" in str(e) or "Table" in str(e) and "not found" in str(e):
            return {}
        raise
