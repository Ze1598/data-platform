"""Tests against the live platform Postgres (not mocks) -- same philosophy
as tests/integration, see its DebugReference.md. Needs the cluster up and
reachable (POSTGRES_HOST/PORT/USER/PASSWORD/DB env vars, defaults match
localhost via the metadata Service's NodePort).
"""

import pandas as pd
import pytest

from metadata_db import delete_row, fetch_lookup, fetch_table, get_engine, insert_row, safe_str, to_json_text, update_row

_TEST_CODE = "test_frontend_crud_roundtrip"


@pytest.fixture
def engine():
    return get_engine()


@pytest.fixture(autouse=True)
def _cleanup(engine):
    # Belt-and-braces: remove any leftover row from a previous failed run
    # before AND after this test, so it's safe to re-run without manual
    # cleanup and doesn't collide with scripts/seed_metadata_db.py's rows.
    # Uses delete_row() itself (the function under test) rather than hand-
    # rolled SQL -- it's idempotent (no-op if the row doesn't exist).
    def _delete():
        delete_row(engine, "source_system", "code", _TEST_CODE)

    _delete()
    yield
    _delete()


def test_get_engine_connects(engine):
    with engine.connect() as conn:
        assert conn.exec_driver_sql("select 1").scalar() == 1


def test_insert_fetch_update_delete_round_trip(engine):
    insert_row(
        engine,
        "source_system",
        {
            "code": _TEST_CODE,
            "name": "Test Source",
            "system_type": "api",
            "connection_config": "{}",
        },
        json_columns={"connection_config"},
    )

    df = fetch_table(engine, "source_system")
    row = df[df["code"] == _TEST_CODE]
    assert len(row) == 1
    assert row.iloc[0]["name"] == "Test Source"
    # uuid columns must come back as strings, not uuid.UUID -- otherwise
    # Streamlit's canvas-based dataframe renderer shows byte-index dicts
    # instead of readable text (see Learnings.md, Phase 1).
    assert isinstance(row.iloc[0]["id"], str)

    row_id = row.iloc[0]["id"]
    update_row(engine, "source_system", "id", row_id, {"name": "Test Source Updated"})
    df = fetch_table(engine, "source_system")
    assert df[df["code"] == _TEST_CODE].iloc[0]["name"] == "Test Source Updated"

    delete_row(engine, "source_system", "id", row_id)
    df = fetch_table(engine, "source_system")
    assert df[df["code"] == _TEST_CODE].empty


def test_fetch_lookup(engine):
    insert_row(
        engine,
        "source_system",
        {"code": _TEST_CODE, "name": "Test Source", "system_type": "api", "connection_config": "{}"},
        json_columns={"connection_config"},
    )
    lookup = fetch_lookup(engine, "source_system")
    assert _TEST_CODE in lookup


def test_safe_str():
    assert safe_str(None) == ""
    assert safe_str(float("nan")) == ""
    assert safe_str("value") == "value"
    assert safe_str(42) == "42"


def test_to_json_text():
    assert to_json_text(None) == "{}"
    assert to_json_text(None, default="[]") == "[]"
    assert to_json_text({"a": 1}) == '{"a": 1}'
    assert to_json_text([1, 2]) == "[1, 2]"
    assert to_json_text('{"already": "json"}') == '{"already": "json"}'
    assert to_json_text(pd.NA) == "{}"
