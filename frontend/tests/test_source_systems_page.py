"""Exercises frontend/pages/1_Source_Systems.py through Streamlit's own
headless script-running harness (AppTest) -- same philosophy as
test_trigger_pipeline_page.py: not just "no exception raised," real widget
interaction plus a live-Postgres round trip check for every write path this
page exposes (Add/Edit/Delete).

Needs the live cluster reachable (same as test_metadata_db.py).

Closes a real coverage gap, not a rewrite of an existing test: this page had
zero AppTest-level coverage before this file -- test_metadata_db.py only
exercises the generic CRUD helper functions (insert_row/update_row/...), not
this page's own top-level code (widget wiring, the connector_kind sentinel
translation, the connection_config JSON validation) -- exactly the class of
bug Learnings.md's "curl-ing a Streamlit page ... is not testing the page"
entry warns is otherwise invisible.
"""

from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from metadata_db import delete_row, get_engine, insert_row

_PAGE_PATH = Path(__file__).resolve().parents[1] / "pages" / "1_Source_Systems.py"
_TEST_CODE = "zz_apptest_source_system"


def _run():
    at = AppTest.from_file(str(_PAGE_PATH))
    at.run()
    return at


@pytest.fixture
def engine():
    return get_engine()


@pytest.fixture(autouse=True)
def _cleanup(engine):
    # Belt-and-braces, same pattern as test_metadata_db.py's _cleanup --
    # remove any leftover row from a previous failed run before AND after,
    # using delete_row() itself (idempotent, no-op if absent).
    def _delete():
        delete_row(engine, "source_system", "code", _TEST_CODE)

    _delete()
    yield
    _delete()


def _fetch_test_row(engine):
    import pandas as pd
    from sqlalchemy import text

    df = pd.read_sql(
        text("select * from source_system where code = :code"), engine, params={"code": _TEST_CODE}
    )
    return df


def test_page_loads_and_lists_source_systems():
    at = _run()
    assert not at.exception, f"Page load raised: {[str(e.value) for e in at.exception]}"
    assert at.dataframe, "Expected the source_system table to render"
    assert at.radio, "Expected the Add/Edit/Delete action radio"
    assert at.radio[0].value == "Add new", "Add new should be the default action"


def test_add_source_system_round_trip(engine):
    at = _run()
    at.text_input(key="add_ss_0_code").set_value(_TEST_CODE).run()
    at.text_input(key="add_ss_0_name").set_value("AppTest Source").run()
    at.selectbox(key="add_ss_0_connector_kind").select("csv").run()
    at.text_input(key="add_ss_0_base_location").set_value("/data-lake/landing/zz_apptest").run()
    at.button(key="add_ss_0_submit").click().run()

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

    df = _fetch_test_row(engine)
    assert len(df) == 1, "Expected exactly one row to have landed in source_system"
    row = df.iloc[0]
    assert row["name"] == "AppTest Source"
    assert row["connector_kind"] == "csv"
    assert row["base_location"] == "/data-lake/landing/zz_apptest"
    assert row["is_active"] is True or bool(row["is_active"]) is True


def test_add_source_system_invalid_connection_config_json_rejected(engine):
    at = _run()
    at.text_input(key="add_ss_0_code").set_value(_TEST_CODE).run()
    at.text_input(key="add_ss_0_name").set_value("AppTest Source").run()
    at.text_area(key="add_ss_0_connection_config").set_value("{not valid json").run()
    at.button(key="add_ss_0_submit").click().run()

    assert not at.exception, f"Submitting invalid JSON raised instead of erroring cleanly: {[str(e.value) for e in at.exception]}"
    assert at.error, "Expected an error message for invalid connection_config JSON"
    assert not at.success, "Should not report success when connection_config JSON is invalid"

    df = _fetch_test_row(engine)
    assert df.empty, "No row should have been created when connection_config JSON is invalid"


def test_add_source_system_missing_required_fields_rejected():
    at = _run()
    # code/name both left blank -- required per build logic in the page
    at.button(key="add_ss_0_submit").click().run()

    assert not at.exception
    assert at.error, "Expected an error for missing code/name"
    assert not at.success


def test_edit_existing_source_system_round_trip(engine):
    insert_row(
        engine,
        "source_system",
        {"code": _TEST_CODE, "name": "Original Name", "system_type": "api", "connection_config": "{}"},
        json_columns={"connection_config"},
    )

    at = _run()
    at.radio[0].set_value("Edit existing").run()
    at.selectbox[0].select(_TEST_CODE).run()

    name_input = next(w for w in at.text_input if w.key and w.key.endswith("_name"))
    name_input.set_value("Updated Name").run()
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

    df = _fetch_test_row(engine)
    assert df.iloc[0]["name"] == "Updated Name"


def test_delete_existing_source_system_round_trip(engine):
    insert_row(
        engine,
        "source_system",
        {"code": _TEST_CODE, "name": "To Be Deleted", "system_type": "api", "connection_config": "{}"},
        json_columns={"connection_config"},
    )

    at = _run()
    at.radio[0].set_value("Delete existing").run()
    at.selectbox[0].select(_TEST_CODE).run()
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

    df = _fetch_test_row(engine)
    assert df.empty, "Row should be gone after Delete"
