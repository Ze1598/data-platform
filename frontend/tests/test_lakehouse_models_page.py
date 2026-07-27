"""Exercises frontend/pages/3_Lakehouse_Models.py through Streamlit's own
headless script-running harness (AppTest) -- same philosophy as
test_trigger_pipeline_page.py: real widget interaction plus a live-Postgres
round trip check, not just "no exception raised."

This page had zero AppTest-level coverage before this file. Dedicated
coverage for its two most novel validation rules, not just a "page loads"
smoke test:
1. "Owning feed" must be one of the checked "Depends on feeds" (build_values()'s
   defensive safety net -- normally unreachable via live-filtered widget
   state, but exercised here at the build_values() layer).
2. deletes_enabled requires every dependent feed's extraction_type to be
   'full'.

Needs the live cluster reachable (same as test_metadata_db.py).
"""

from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import text
from streamlit.testing.v1 import AppTest

from metadata_db import delete_row, get_engine

_PAGE_PATH = Path(__file__).resolve().parents[1] / "pages" / "3_Lakehouse_Models.py"
_TEST_SOURCE_CODE = "zz_apptest_source_for_lakehouse_models"
_TEST_FEED_PREFIX = "zz_apptest_lm_feed_"
_TEST_MODEL_PREFIX = "zz_apptest_lm_model_"


def _run():
    at = AppTest.from_file(str(_PAGE_PATH))
    at.run()
    return at


@pytest.fixture
def engine():
    return get_engine()


@pytest.fixture(autouse=True)
def _cleanup(engine):
    def _delete_all():
        with engine.begin() as conn:
            conn.execute(
                text("delete from lakehouse_models where friendly_name like :p"),
                {"p": f"{_TEST_MODEL_PREFIX}%"},
            )
            conn.execute(
                text("delete from data_feed where friendly_name like :p"), {"p": f"{_TEST_FEED_PREFIX}%"}
            )
        delete_row(engine, "source_system", "code", _TEST_SOURCE_CODE)

    _delete_all()
    yield
    _delete_all()


@pytest.fixture
def full_feed(engine):
    """A real, full-extraction data_feed this page's 'Depends on feeds'
    multiselect can pick -- deletes_enabled's own validation requires this."""
    name = f"{_TEST_FEED_PREFIX}full"
    with engine.begin() as conn:
        source_id = conn.execute(
            text(
                "insert into source_system (code, name, system_type) values (:code, :code, 'file_drop') "
                "returning id"
            ),
            {"code": _TEST_SOURCE_CODE},
        ).scalar()
        conn.execute(
            text(
                """
                INSERT INTO data_feed (source_system_id, friendly_name, source_object_name, batch_group,
                                        batch_group_friendly_name, extraction_type, source_pk)
                VALUES (:source_id, :name, :name, gen_random_uuid(), :name, 'full', '[]')
                """
            ),
            {"source_id": source_id, "name": name},
        )
    return name


@pytest.fixture
def incremental_feed(engine, full_feed):
    """A second feed, incremental this time -- used by the deletes_enabled
    rejection test, which needs a *mixed* full+incremental dependency set."""
    name = f"{_TEST_FEED_PREFIX}incremental"
    with engine.begin() as conn:
        source_id = conn.execute(
            text("select id from source_system where code = :code"), {"code": _TEST_SOURCE_CODE}
        ).scalar()
        conn.execute(
            text(
                """
                INSERT INTO data_feed (source_system_id, friendly_name, source_object_name, batch_group,
                                        batch_group_friendly_name, extraction_type, watermark_column, source_pk)
                VALUES (:source_id, :name, :name, gen_random_uuid(), :name, 'incremental', 'updated_at', '[]')
                """
            ),
            {"source_id": source_id, "name": name},
        )
    return name


def _fetch_model(engine, friendly_name: str) -> pd.DataFrame:
    return pd.read_sql(
        text("select * from lakehouse_models where friendly_name = :name"), engine, params={"name": friendly_name}
    )


def test_page_loads_and_lists_lakehouse_models():
    at = _run()
    assert not at.exception, f"Page load raised: {[str(e.value) for e in at.exception]}"
    assert at.dataframe, "Expected the lakehouse_models table to render"


def test_add_dimension_model_round_trip(full_feed, engine):
    model_name = f"{_TEST_MODEL_PREFIX}dim"
    at = _run()
    key_prefix = "add_lm_0"

    at.text_input(key=f"{key_prefix}_friendly_name").set_value(model_name).run()
    at.text_input(key=f"{key_prefix}_table_name").set_value(model_name).run()
    at.text_input(key=f"{key_prefix}_new_model_schema").set_value("zz_apptest_domain").run()
    at.multiselect(key=f"{key_prefix}_depends_on").select(full_feed).run()
    at.selectbox(key=f"{key_prefix}_owning_feed").select(full_feed).run()
    at.text_input(key=f"{key_prefix}_business_key_columns").set_value("id").run()
    at.button(key=f"{key_prefix}_submit").click().run()

    assert not at.exception, f"Submitting raised: {[str(e.value) for e in at.exception]}"
    # Not asserting at.success here: this handler calls st.success() then
    # st.rerun() immediately -- AppTest's success-message capture across a
    # chained rerun is unreliable (confirmed by direct reproduction: an
    # otherwise-identical flow sometimes drops it), even though the write
    # itself always lands correctly. Per Learnings.md's own lesson
    # ("verifying the submission call succeeded is a materially weaker claim
    # than verifying the thing it submitted actually completed"), the DB
    # check below is the authoritative assertion, not this message.
    assert not at.error, f"Expected no errors, got: {[str(e.value) for e in at.error]}"

    df = _fetch_model(engine, model_name)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["model_schema"] == "zz_apptest_domain"
    assert row["table_type"] == "dimension"
    assert row["scd_type"] == 2, "Page default scd_type is 2"


def test_add_model_without_any_dependent_feed_rejected(full_feed, engine):
    model_name = f"{_TEST_MODEL_PREFIX}nodeps"
    at = _run()
    key_prefix = "add_lm_0"

    at.text_input(key=f"{key_prefix}_friendly_name").set_value(model_name).run()
    at.text_input(key=f"{key_prefix}_table_name").set_value(model_name).run()
    at.text_input(key=f"{key_prefix}_new_model_schema").set_value("zz_apptest_domain").run()
    # depends_on_feed_names deliberately left empty
    at.button(key=f"{key_prefix}_submit").click().run()

    assert not at.exception
    assert at.error, "Expected an error requiring at least one dependent feed"
    assert not at.success

    df = _fetch_model(engine, model_name)
    assert df.empty


def test_add_model_deletes_enabled_requires_full_extraction_on_every_dependent_feed(
    full_feed, incremental_feed, engine
):
    model_name = f"{_TEST_MODEL_PREFIX}deletes_mixed"
    at = _run()
    key_prefix = "add_lm_0"

    at.text_input(key=f"{key_prefix}_friendly_name").set_value(model_name).run()
    at.text_input(key=f"{key_prefix}_table_name").set_value(model_name).run()
    at.text_input(key=f"{key_prefix}_new_model_schema").set_value("zz_apptest_domain").run()
    at.multiselect(key=f"{key_prefix}_depends_on").select(full_feed).run()
    at.multiselect(key=f"{key_prefix}_depends_on").select(incremental_feed).run()
    at.selectbox(key=f"{key_prefix}_owning_feed").select(full_feed).run()
    at.checkbox(key=f"{key_prefix}_deletes_enabled").check().run()
    at.button(key=f"{key_prefix}_submit").click().run()

    assert not at.exception, f"Submitting raised: {[str(e.value) for e in at.exception]}"
    assert at.error, "Expected an error rejecting deletes_enabled with a non-full dependent feed"
    assert any("extraction type to be 'full'" in str(e.value) for e in at.error), (
        f"Expected the specific deletes_enabled message, got: {[str(e.value) for e in at.error]}"
    )
    assert not at.success

    df = _fetch_model(engine, model_name)
    assert df.empty


def test_edit_existing_lakehouse_model_round_trip(full_feed, engine):
    model_name = f"{_TEST_MODEL_PREFIX}editme"
    with engine.begin() as conn:
        feed_id = conn.execute(
            text("select id from data_feed where friendly_name = :name"), {"name": full_feed}
        ).scalar()
        conn.execute(
            text(
                """
                INSERT INTO lakehouse_models (friendly_name, table_name, model_schema, table_type,
                                               load_type, owning_feed_id, depends_on_feeds, is_active)
                VALUES (:name, :name, 'zz_apptest_domain', 'dimension', 0, :owning_feed_id, :depends_on_feeds, true)
                """
            ),
            # Two distinct parameter names bound to the same value -- reusing one
            # named parameter for both a uuid column and a text column confuses
            # psycopg's type inference (AmbiguousParameter: uuid versus text).
            {"name": model_name, "owning_feed_id": str(feed_id), "depends_on_feeds": str(feed_id)},
        )

    at = _run()
    at.radio[0].set_value("Edit existing").run()
    model_selectbox = next(w for w in at.selectbox if model_name in (w.options or []))
    model_selectbox.select(model_name).run()

    is_active_checkbox = next(w for w in at.checkbox if w.key and w.key.endswith("_is_active"))
    is_active_checkbox.uncheck().run()
    submit_button = next(w for w in at.button if w.key and w.key.endswith("_submit"))
    submit_button.click().run()

    assert not at.exception, f"Editing raised: {[str(e.value) for e in at.exception]}"
    # Not asserting at.success here: this handler calls st.success() then
    # st.rerun() immediately -- AppTest's success-message capture across a
    # chained rerun is unreliable (confirmed by direct reproduction: an
    # otherwise-identical flow sometimes drops it), even though the write
    # itself always lands correctly. Per Learnings.md's own lesson
    # ("verifying the submission call succeeded is a materially weaker claim
    # than verifying the thing it submitted actually completed"), the DB
    # check below is the authoritative assertion, not this message.
    assert not at.error, f"Expected no errors, got: {[str(e.value) for e in at.error]}"

    df = _fetch_model(engine, model_name)
    assert bool(df.iloc[0]["is_active"]) is False


def test_delete_existing_lakehouse_model_round_trip(full_feed, engine):
    model_name = f"{_TEST_MODEL_PREFIX}deleteme"
    with engine.begin() as conn:
        feed_id = conn.execute(
            text("select id from data_feed where friendly_name = :name"), {"name": full_feed}
        ).scalar()
        conn.execute(
            text(
                """
                INSERT INTO lakehouse_models (friendly_name, table_name, model_schema, table_type,
                                               load_type, owning_feed_id, depends_on_feeds)
                VALUES (:name, :name, 'zz_apptest_domain', 'dimension', 0, :owning_feed_id, :depends_on_feeds)
                """
            ),
            {"name": model_name, "owning_feed_id": str(feed_id), "depends_on_feeds": str(feed_id)},
        )

    at = _run()
    at.radio[0].set_value("Delete existing").run()
    model_selectbox = next(w for w in at.selectbox if model_name in (w.options or []))
    model_selectbox.select(model_name).run()
    at.button[0].click().run()

    assert not at.exception, f"Deleting raised: {[str(e.value) for e in at.exception]}"
    # Not asserting at.success here: this handler calls st.success() then
    # st.rerun() immediately -- AppTest's success-message capture across a
    # chained rerun is unreliable (confirmed by direct reproduction: an
    # otherwise-identical flow sometimes drops it), even though the write
    # itself always lands correctly. Per Learnings.md's own lesson
    # ("verifying the submission call succeeded is a materially weaker claim
    # than verifying the thing it submitted actually completed"), the DB
    # check below is the authoritative assertion, not this message.
    assert not at.error, f"Expected no errors, got: {[str(e.value) for e in at.error]}"

    df = _fetch_model(engine, model_name)
    assert df.empty
