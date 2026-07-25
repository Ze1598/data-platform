"""Tests against the live platform Postgres (not mocks) -- same philosophy
as tests/integration, see its DebugReference.md. Needs the cluster up and
reachable (POSTGRES_HOST/PORT/USER/PASSWORD/DB env vars, defaults match
localhost via the metadata Service's NodePort).
"""

import pandas as pd
import pytest
from sqlalchemy import text

from metadata_db import (
    delete_row,
    fetch_lakehouse_model_columns,
    fetch_lookup,
    fetch_table,
    get_engine,
    insert_row,
    replace_lakehouse_model_columns,
    safe_str,
    to_json_text,
    update_row,
)

_TEST_CODE = "test_frontend_crud_roundtrip"
_TEST_MODEL_COLUMNS_CODE = "test_lakehouse_model_columns_roundtrip"


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


@pytest.fixture
def lakehouse_model_columns_fixture(engine):
    """A throwaway source_system -> data_feed -> lakehouse_models chain,
    scoped entirely to this test -- lakehouse_model_columns rows need a
    real model_id/source_feed_id to reference (both real FKs), unlike
    source_system's standalone round-trip test above. Torn down in
    reverse FK order regardless of test outcome."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO source_system (code, name, system_type) VALUES (:code, :name, 'file_drop')"
            ),
            {"code": _TEST_MODEL_COLUMNS_CODE, "name": "Test source"},
        )
        feed_id = conn.execute(
            text(
                """
                INSERT INTO data_feed (source_system_id, friendly_name, source_object_name, batch_group,
                                        batch_group_friendly_name, extraction_type, source_pk)
                VALUES (
                    (SELECT id FROM source_system WHERE code = :code),
                    :friendly_name, :friendly_name, gen_random_uuid(), :friendly_name, 'full', '[]'
                )
                RETURNING id
                """
            ),
            {"code": _TEST_MODEL_COLUMNS_CODE, "friendly_name": _TEST_MODEL_COLUMNS_CODE},
        ).scalar()
        model_id = conn.execute(
            text(
                """
                INSERT INTO lakehouse_models (friendly_name, table_name, model_schema, table_type,
                                               load_type, owning_feed_id)
                VALUES (:name, :name, 'test_domain', 'dimension', 0, :feed_id)
                RETURNING id
                """
            ),
            {"name": _TEST_MODEL_COLUMNS_CODE, "feed_id": feed_id},
        ).scalar()

    yield {"model_id": str(model_id), "feed_id": str(feed_id)}

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM lakehouse_model_columns WHERE model_id = :id"), {"id": model_id})
        conn.execute(text("DELETE FROM lakehouse_models WHERE id = :id"), {"id": model_id})
        conn.execute(text("DELETE FROM data_feed WHERE id = :id"), {"id": feed_id})
        conn.execute(text("DELETE FROM source_system WHERE code = :code"), {"code": _TEST_MODEL_COLUMNS_CODE})


def test_replace_and_fetch_lakehouse_model_columns_round_trip(engine, lakehouse_model_columns_fixture):
    model_id = lakehouse_model_columns_fixture["model_id"]
    feed_id = lakehouse_model_columns_fixture["feed_id"]

    replace_lakehouse_model_columns(
        engine,
        model_id,
        [
            {
                "column_name": "id", "source_feed_id": feed_id, "data_type": "string",
                "is_nullable": False, "is_business_key": True, "is_tracked": False, "ordinal_position": 0,
            },
            {
                "column_name": "amount", "source_feed_id": feed_id, "data_type": "double",
                "is_nullable": True, "is_business_key": False, "is_tracked": True, "ordinal_position": 1,
            },
        ],
    )

    df = fetch_lakehouse_model_columns(engine, model_id)
    assert list(df["column_name"]) == ["id", "amount"]
    id_row = df[df["column_name"] == "id"].iloc[0]
    assert id_row["is_business_key"] and not id_row["is_tracked"]

    # A second call fully replaces the prior set, not appends to it --
    # the frontend page's editor grid always submits the complete,
    # currently-intended column list on save.
    replace_lakehouse_model_columns(
        engine,
        model_id,
        [
            {
                "column_name": "id", "source_feed_id": feed_id, "data_type": "string",
                "is_nullable": False, "is_business_key": True, "is_tracked": False, "ordinal_position": 0,
            },
        ],
    )
    df = fetch_lakehouse_model_columns(engine, model_id)
    assert list(df["column_name"]) == ["id"]


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
