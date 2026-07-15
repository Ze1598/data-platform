"""Live Postgres catalog introspection for PostgresConnector.discover_primary_key()
-- the ODS layer's real-key-discovery path (see Roadmap.md "ODS layer",
metadata/DataModel.md). Exercises real pg_index/pg_attribute lookups
against throwaway tables, not mocked -- this is exactly the kind of
"does the SQL actually work against real Postgres" question a unit test
can't answer.
"""

import os

import pytest
from connectors.postgres import PostgresConnector


def _connector(*, query: str, table_name: str | None) -> PostgresConnector:
    return PostgresConnector(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
        user=os.environ.get("POSTGRES_USER", "platform"),
        password=os.environ.get("POSTGRES_PASSWORD", "platform"),
        query=query,
        table_name=table_name,
    )


@pytest.fixture
def scratch_tables(metadata_conn):
    with metadata_conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS ods_test_single_pk")
        cur.execute("CREATE TABLE ods_test_single_pk (id bigint PRIMARY KEY, name text)")

        cur.execute("DROP TABLE IF EXISTS ods_test_composite_pk")
        cur.execute(
            "CREATE TABLE ods_test_composite_pk (tenant_id bigint, item_id bigint, label text, "
            "PRIMARY KEY (tenant_id, item_id))"
        )

        cur.execute("DROP TABLE IF EXISTS ods_test_no_pk")
        cur.execute("CREATE TABLE ods_test_no_pk (id bigint, name text)")
    metadata_conn.commit()

    yield

    with metadata_conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS ods_test_single_pk")
        cur.execute("DROP TABLE IF EXISTS ods_test_composite_pk")
        cur.execute("DROP TABLE IF EXISTS ods_test_no_pk")
    metadata_conn.commit()


def test_single_column_primary_key(scratch_tables):
    connector = _connector(query="select * from ods_test_single_pk", table_name="ods_test_single_pk")
    assert connector.discover_primary_key() == ["id"]


def test_composite_primary_key_preserves_declared_order(scratch_tables):
    connector = _connector(query="select * from ods_test_composite_pk", table_name="ods_test_composite_pk")
    assert connector.discover_primary_key() == ["tenant_id", "item_id"]


def test_table_with_no_primary_key_returns_empty_list(scratch_tables):
    connector = _connector(query="select * from ods_test_no_pk", table_name="ods_test_no_pk")
    assert connector.discover_primary_key() == []


def test_multi_table_query_with_no_table_name_never_queries_at_all():
    # Mirrors metadata_runs' real shape: a genuine multi-table LEFT JOIN,
    # with no single table for discover_primary_key() to introspect --
    # table_name is never set for it. Deliberately points `query` at
    # nonsense to prove this really never executes (a real connection
    # attempt against this query would fail loudly, not return None).
    connector = _connector(
        query="select * from data_processing_runs r left join data_feed df on df.id = r.data_feed_id "
        "left join lakehouse_models lm on lm.friendly_name = r.model_key",
        table_name=None,
    )
    assert connector.discover_primary_key() is None
