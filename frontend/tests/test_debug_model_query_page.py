"""Exercises frontend/pages/8_Debug_Model_Query.py through Streamlit's own
headless script-running harness (AppTest) -- same philosophy as every other
page test in this suite: real widget interaction against live infrastructure,
not just "no exception raised."

Needs the live cluster reachable -- Trino via its NodePort (TRINO_HOST/
TRINO_PORT, defaults matching dbt's own profiles.yml / tests/integration's
convention: localhost:8080), same as every other host-side check in this
repo.
"""

import os
from pathlib import Path

import pytest
import trino.dbapi
from streamlit.testing.v1 import AppTest

_PAGE_PATH = Path(__file__).resolve().parents[1] / "pages" / "8_Debug_Model_Query.py"


def _run():
    at = AppTest.from_file(str(_PAGE_PATH), default_timeout=30)
    at.run()
    return at


@pytest.fixture
def trino_conn():
    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="test_debug_model_query_page",
        catalog="iceberg",
    )
    yield conn
    conn.close()


def test_page_loads_and_lists_model_tables(trino_conn):
    cur = trino_conn.cursor()
    cur.execute("SHOW TABLES FROM iceberg.model")
    real_tables = sorted(row[0] for row in cur.fetchall())
    if not real_tables:
        pytest.skip("No tables in the model schema in this environment")

    at = _run()
    assert not at.exception, f"Page load raised: {[str(e.value) for e in at.exception]}"
    assert at.selectbox, "Expected a table picker selectbox"
    assert set(at.selectbox[0].options) == set(real_tables)


def test_selecting_a_table_renders_matching_columns(trino_conn):
    cur = trino_conn.cursor()
    cur.execute("SHOW TABLES FROM iceberg.model")
    real_tables = sorted(row[0] for row in cur.fetchall())
    if not real_tables:
        pytest.skip("No tables in the model schema in this environment")
    table = real_tables[0]

    cur = trino_conn.cursor()
    cur.execute(f"DESCRIBE iceberg.model.{table}")
    expected_columns = {row[0] for row in cur.fetchall()}

    at = _run()
    at.selectbox[0].select(table).run()

    assert not at.exception, f"Selecting {table!r} raised: {[str(e.value) for e in at.exception]}"
    assert at.dataframe, "Expected the query result table to render"
    rendered_columns = set(at.dataframe[0].value.columns)
    assert rendered_columns == expected_columns
