"""Exercises frontend/pages/2_Data_Feeds.py through Streamlit's own headless
script-running harness (AppTest) -- same philosophy as
test_trigger_pipeline_page.py: real widget interaction plus a live-Postgres
round trip check, not just "no exception raised."

This page had zero AppTest-level coverage before this file. Its two most
novel, easy-to-silently-break validation rules get dedicated coverage, not
just a "page loads" smoke test:
1. incremental extraction requires a watermark_column (build_values()).
2. a batch_group is expected to map to exactly one ODS domain (1:1) --
   see Progress.md's "Five-item backlog batch (2026-07-17)" entry and
   metadata/DataModel.md.

Needs the live cluster reachable (same as test_metadata_db.py).
"""

from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import text
from streamlit.testing.v1 import AppTest

from metadata_db import delete_row, get_engine, insert_row

_PAGE_PATH = Path(__file__).resolve().parents[1] / "pages" / "2_Data_Feeds.py"
_TEST_SOURCE_CODE = "zz_apptest_source_for_data_feeds"
_TEST_FEED_PREFIX = "zz_apptest_feed_"


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
                text("delete from data_feed where friendly_name like :pattern"),
                {"pattern": f"{_TEST_FEED_PREFIX}%"},
            )
        delete_row(engine, "source_system", "code", _TEST_SOURCE_CODE)

    _delete_all()
    yield
    _delete_all()


@pytest.fixture
def source_system_row(engine):
    insert_row(
        engine,
        "source_system",
        {"code": _TEST_SOURCE_CODE, "name": "AppTest Source For Data Feeds", "system_type": "file_drop"},
    )
    df = pd.read_sql(
        text("select code from source_system where code = :code"), engine, params={"code": _TEST_SOURCE_CODE}
    )
    return df.iloc[0]["code"]


def _fetch_feed(engine, friendly_name: str) -> pd.DataFrame:
    return pd.read_sql(
        text("select * from data_feed where friendly_name = :name"), engine, params={"name": friendly_name}
    )


def test_page_loads_and_lists_data_feeds():
    at = _run()
    assert not at.exception, f"Page load raised: {[str(e.value) for e in at.exception]}"
    assert at.dataframe, "Expected the data_feed table to render"


def test_add_data_feed_full_extraction_new_batch_round_trip(source_system_row, engine):
    feed_name = f"{_TEST_FEED_PREFIX}full"
    batch_name = f"{_TEST_FEED_PREFIX}batch_full"
    at = _run()
    key_prefix = "add_df_0"

    at.selectbox(key=f"{key_prefix}_source_code").select(_TEST_SOURCE_CODE).run()
    at.text_input(key=f"{key_prefix}_friendly_name").set_value(feed_name).run()
    at.text_input(key=f"{key_prefix}_source_object_name").set_value("zz_apptest_table").run()
    # NEW_BATCH_OPTION is already the default choice when no prior selection
    # exists -- fill the new-batch-name field it reveals.
    at.text_input(key=f"{key_prefix}_new_batch_name").set_value(batch_name).run()
    # extraction_type defaults to "full", which needs no watermark_column.
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

    df = _fetch_feed(engine, feed_name)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["batch_group_friendly_name"] == batch_name
    assert row["extraction_type"] == "full"
    assert row["source_object_name"] == "zz_apptest_table"
    assert row["pipeline_steps"] == "0,1,2", "Default pipeline_step_labels should map to all three step ids"


def test_add_data_feed_incremental_without_watermark_rejected(source_system_row, engine):
    feed_name = f"{_TEST_FEED_PREFIX}incremental_bad"
    at = _run()
    key_prefix = "add_df_0"

    at.selectbox(key=f"{key_prefix}_source_code").select(_TEST_SOURCE_CODE).run()
    at.text_input(key=f"{key_prefix}_friendly_name").set_value(feed_name).run()
    at.text_input(key=f"{key_prefix}_source_object_name").set_value("zz_apptest_table").run()
    at.text_input(key=f"{key_prefix}_new_batch_name").set_value(f"{_TEST_FEED_PREFIX}batch_incr").run()
    at.selectbox(key=f"{key_prefix}_extraction_type").select("incremental").run()
    # watermark_column deliberately left blank
    at.button(key=f"{key_prefix}_submit").click().run()

    assert not at.exception
    assert at.error, "Expected an error requiring watermark_column for incremental extraction"
    assert not at.success

    df = _fetch_feed(engine, feed_name)
    assert df.empty, "No row should be created when watermark_column is missing for incremental extraction"


def test_add_data_feed_ods_domain_name_required_when_ods_enabled(source_system_row, engine):
    feed_name = f"{_TEST_FEED_PREFIX}ods_no_name"
    at = _run()
    key_prefix = "add_df_0"

    at.selectbox(key=f"{key_prefix}_source_code").select(_TEST_SOURCE_CODE).run()
    at.text_input(key=f"{key_prefix}_friendly_name").set_value(feed_name).run()
    at.text_input(key=f"{key_prefix}_source_object_name").set_value("zz_apptest_table").run()
    at.text_input(key=f"{key_prefix}_new_batch_name").set_value(f"{_TEST_FEED_PREFIX}batch_ods1").run()
    at.checkbox(key=f"{key_prefix}_ods_enabled").check().run()
    # batch_ods_name defaults to the batch's own friendly name once ODS is
    # enabled -- explicitly blank it to hit the "required" branch.
    at.text_input(key=f"{key_prefix}_batch_ods_name").set_value("").run()
    at.button(key=f"{key_prefix}_submit").click().run()

    assert not at.exception
    assert at.error, "Expected an error requiring an ODS domain name"
    assert not at.success

    df = _fetch_feed(engine, feed_name)
    assert df.empty


def test_add_data_feed_ods_domain_name_invalid_slug_rejected(source_system_row, engine):
    feed_name = f"{_TEST_FEED_PREFIX}ods_bad_slug"
    at = _run()
    key_prefix = "add_df_0"

    at.selectbox(key=f"{key_prefix}_source_code").select(_TEST_SOURCE_CODE).run()
    at.text_input(key=f"{key_prefix}_friendly_name").set_value(feed_name).run()
    at.text_input(key=f"{key_prefix}_source_object_name").set_value("zz_apptest_table").run()
    at.text_input(key=f"{key_prefix}_new_batch_name").set_value(f"{_TEST_FEED_PREFIX}batch_ods2").run()
    at.checkbox(key=f"{key_prefix}_ods_enabled").check().run()
    at.text_input(key=f"{key_prefix}_batch_ods_name").set_value("Not A Valid Slug!").run()
    at.button(key=f"{key_prefix}_submit").click().run()

    assert not at.exception
    assert at.error, "Expected an error rejecting an invalid ODS domain slug"
    assert not at.success

    df = _fetch_feed(engine, feed_name)
    assert df.empty


def test_ods_domain_conflict_across_shared_batch_group_rejected(source_system_row, engine):
    """A batch_group is expected to map to exactly one ODS domain (1:1) --
    seed one feed with ods_enabled + a real batch_ods_name via direct SQL,
    then attempt to add a second feed sharing the same batch_group with a
    *different* batch_ods_name through the page -- build_values() must
    reject it, not silently create a conflicting second ODS domain for the
    same batch."""
    shared_batch_name = f"{_TEST_FEED_PREFIX}shared_batch"
    first_feed_name = f"{_TEST_FEED_PREFIX}ods_first"
    second_feed_name = f"{_TEST_FEED_PREFIX}ods_second"

    with engine.begin() as conn:
        source_id = conn.execute(
            text("select id from source_system where code = :code"), {"code": _TEST_SOURCE_CODE}
        ).scalar()
        batch_group_id = conn.execute(
            text(
                """
                INSERT INTO data_feed (source_system_id, friendly_name, source_object_name, batch_group,
                                        batch_group_friendly_name, extraction_type, source_pk,
                                        ods_enabled, batch_ods_name)
                VALUES (:source_id, :name, :name, gen_random_uuid(), :batch_name, 'full', '[]', true, :ods_name)
                RETURNING batch_group
                """
            ),
            {
                "source_id": source_id,
                "name": first_feed_name,
                "batch_name": shared_batch_name,
                "ods_name": "zz_apptest_ods_a",
            },
        ).scalar()

    at = _run()
    key_prefix = "add_df_0"
    at.selectbox(key=f"{key_prefix}_source_code").select(_TEST_SOURCE_CODE).run()
    at.text_input(key=f"{key_prefix}_friendly_name").set_value(second_feed_name).run()
    at.text_input(key=f"{key_prefix}_source_object_name").set_value("zz_apptest_table_2").run()
    # Pick the EXISTING shared batch group, not a new one.
    at.selectbox(key=f"{key_prefix}_batch_choice").select(shared_batch_name).run()
    at.checkbox(key=f"{key_prefix}_ods_enabled").check().run()
    at.text_input(key=f"{key_prefix}_batch_ods_name").set_value("zz_apptest_ods_b").run()
    at.button(key=f"{key_prefix}_submit").click().run()

    assert not at.exception, f"Submitting raised: {[str(e.value) for e in at.exception]}"
    assert at.error, "Expected an ODS-domain-conflict error"
    assert any("already has ODS domain" in str(e.value) for e in at.error), (
        f"Expected the specific ODS-conflict message, got: {[str(e.value) for e in at.error]}"
    )
    assert not at.success

    df = _fetch_feed(engine, second_feed_name)
    assert df.empty, "The conflicting second feed must not have been created"


def test_edit_existing_data_feed_round_trip(source_system_row, engine):
    feed_name = f"{_TEST_FEED_PREFIX}editme"
    with engine.begin() as conn:
        source_id = conn.execute(
            text("select id from source_system where code = :code"), {"code": _TEST_SOURCE_CODE}
        ).scalar()
        conn.execute(
            text(
                """
                INSERT INTO data_feed (source_system_id, friendly_name, source_object_name, batch_group,
                                        batch_group_friendly_name, extraction_type, source_pk, is_active)
                VALUES (:source_id, :name, :name, gen_random_uuid(), :name, 'full', '[]', true)
                """
            ),
            {"source_id": source_id, "name": feed_name},
        )

    at = _run()
    at.radio[0].set_value("Edit existing").run()
    feed_selectbox = next(w for w in at.selectbox if w.value == feed_name or feed_name in (w.options or []))
    feed_selectbox.select(feed_name).run()

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

    df = _fetch_feed(engine, feed_name)
    assert bool(df.iloc[0]["is_active"]) is False


def test_delete_existing_data_feed_round_trip(source_system_row, engine):
    feed_name = f"{_TEST_FEED_PREFIX}deleteme"
    with engine.begin() as conn:
        source_id = conn.execute(
            text("select id from source_system where code = :code"), {"code": _TEST_SOURCE_CODE}
        ).scalar()
        conn.execute(
            text(
                """
                INSERT INTO data_feed (source_system_id, friendly_name, source_object_name, batch_group,
                                        batch_group_friendly_name, extraction_type, source_pk)
                VALUES (:source_id, :name, :name, gen_random_uuid(), :name, 'full', '[]')
                """
            ),
            {"source_id": source_id, "name": feed_name},
        )

    at = _run()
    at.radio[0].set_value("Delete existing").run()
    feed_selectbox = next(w for w in at.selectbox if feed_name in (w.options or []))
    feed_selectbox.select(feed_name).run()
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

    df = _fetch_feed(engine, feed_name)
    assert df.empty
