"""Exercises frontend/pages/4_Ingestion_Triggers.py through Streamlit's own
headless script-running harness (AppTest) -- same philosophy as
test_trigger_pipeline_page.py: real widget interaction plus a live-Postgres
round trip check, not just "no exception raised."

This page had zero test coverage of any kind before this file. Dedicated
coverage for its two DB-enforced rules plus one application-layer-only rule
(metadata/db/init/01_platform_metadata.sql, chk_ingestion_triggers_cron,
chk_ingestion_triggers_sensor_feed_only, uq_ingestion_triggers_controlling_object):
1. a sensor-type trigger is only valid for a feed whose source system's
   connector_kind is 'csv'/'json_file' (application-layer only -- reaches
   source_system through two joins, can't be a DB constraint).
2. a sensor-type trigger is only valid for controlling_object_type='feed',
   never 'model' (DB-enforced too, but the frontend's own error path is
   exercised here independently).
3. at most one trigger per controlled feed/model (uq_ingestion_triggers_
   controlling_object) -- not directly tested here since every test uses
   its own freshly-created, trigger-free scratch feed/model, avoiding the
   constraint entirely rather than exercising the DB's own rejection of it.

Needs the live cluster reachable (same as test_metadata_db.py).
"""

from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import text
from streamlit.testing.v1 import AppTest

from metadata_db import delete_row, get_engine

_PAGE_PATH = Path(__file__).resolve().parents[1] / "pages" / "4_Ingestion_Triggers.py"
_TEST_SOURCE_PREFIX = "zz_apptest_it_source_"
_TEST_FEED_PREFIX = "zz_apptest_it_feed_"
_TEST_MODEL_PREFIX = "zz_apptest_it_model_"


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
                text(
                    "delete from ingestion_triggers where controlling_object_id in "
                    "(select id from data_feed where friendly_name like :fp) "
                    "or controlling_object_id in (select id from lakehouse_models where friendly_name like :mp)"
                ),
                {"fp": f"{_TEST_FEED_PREFIX}%", "mp": f"{_TEST_MODEL_PREFIX}%"},
            )
            conn.execute(text("delete from lakehouse_models where friendly_name like :p"), {"p": f"{_TEST_MODEL_PREFIX}%"})
            conn.execute(text("delete from data_feed where friendly_name like :p"), {"p": f"{_TEST_FEED_PREFIX}%"})
            conn.execute(text("delete from source_system where code like :p"), {"p": f"{_TEST_SOURCE_PREFIX}%"})

    _delete_all()
    yield
    _delete_all()


@pytest.fixture
def scratch_feeds(engine):
    """A sensor-eligible (csv connector_kind) and a sensor-ineligible
    (postgres connector_kind) feed, both brand-new and trigger-free --
    avoids uq_ingestion_triggers_controlling_object entirely by construction,
    and avoids any dependency on which real seeded feeds happen to already
    have a trigger today."""
    csv_source_code = f"{_TEST_SOURCE_PREFIX}csv"
    pg_source_code = f"{_TEST_SOURCE_PREFIX}pg"
    csv_feed_name = f"{_TEST_FEED_PREFIX}csv"
    pg_feed_name = f"{_TEST_FEED_PREFIX}pg"

    with engine.begin() as conn:
        csv_source_id = conn.execute(
            text(
                "insert into source_system (code, name, system_type, connector_kind) "
                "values (:code, :code, 'file_drop', 'csv') returning id"
            ),
            {"code": csv_source_code},
        ).scalar()
        pg_source_id = conn.execute(
            text(
                "insert into source_system (code, name, system_type, connector_kind) "
                "values (:code, :code, 'database', 'postgres') returning id"
            ),
            {"code": pg_source_code},
        ).scalar()
        conn.execute(
            text(
                """
                INSERT INTO data_feed (source_system_id, friendly_name, source_object_name, batch_group,
                                        batch_group_friendly_name, extraction_type, source_pk)
                VALUES (:sid, :name, :name, gen_random_uuid(), :name, 'full', '[]')
                """
            ),
            {"sid": csv_source_id, "name": csv_feed_name},
        )
        conn.execute(
            text(
                """
                INSERT INTO data_feed (source_system_id, friendly_name, source_object_name, batch_group,
                                        batch_group_friendly_name, extraction_type, source_pk)
                VALUES (:sid, :name, :name, gen_random_uuid(), :name, 'full', '[]')
                """
            ),
            {"sid": pg_source_id, "name": pg_feed_name},
        )
    return {"csv_feed": csv_feed_name, "postgres_feed": pg_feed_name}


@pytest.fixture
def scratch_model(engine, scratch_feeds):
    model_name = f"{_TEST_MODEL_PREFIX}dim"
    with engine.begin() as conn:
        feed_id = conn.execute(
            text("select id from data_feed where friendly_name = :name"), {"name": scratch_feeds["csv_feed"]}
        ).scalar()
        conn.execute(
            text(
                """
                INSERT INTO lakehouse_models (friendly_name, table_name, model_schema, table_type,
                                               load_type, owning_feed_id, depends_on_feeds)
                VALUES (:name, :name, 'zz_apptest_domain', 'dimension', 0, :owning_feed_id, :depends_on_feeds)
                """
            ),
            # Two distinct parameter names bound to the same value -- reusing one
            # named parameter for both a uuid column and a text column confuses
            # psycopg's type inference (AmbiguousParameter: uuid versus text).
            {"name": model_name, "owning_feed_id": str(feed_id), "depends_on_feeds": str(feed_id)},
        )
    return model_name


def _fetch_trigger_for(engine, controlling_object_type: str, target_name: str) -> pd.DataFrame:
    table = "data_feed" if controlling_object_type == "feed" else "lakehouse_models"
    return pd.read_sql(
        text(
            f"select it.* from ingestion_triggers it "
            f"join {table} t on t.id = it.controlling_object_id "
            f"where t.friendly_name = :name and it.controlling_object_type = :type"
        ),
        engine,
        params={"name": target_name, "type": controlling_object_type},
    )


def test_page_loads_and_lists_triggers():
    at = _run()
    assert not at.exception, f"Page load raised: {[str(e.value) for e in at.exception]}"
    assert at.dataframe, "Expected the ingestion_triggers table to render"
    assert at.radio, "Expected the Add/Edit/Delete action radio"


def test_add_schedule_trigger_for_feed_round_trip(scratch_feeds, engine):
    at = _run()
    key_prefix = "add_it_0"
    at.selectbox(key=f"{key_prefix}_target_name").select(scratch_feeds["csv_feed"]).run()
    # trigger_type defaults to "schedule"
    at.text_input(key=f"{key_prefix}_cron").set_value("0 6 * * *").run()
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

    df = _fetch_trigger_for(engine, "feed", scratch_feeds["csv_feed"])
    assert len(df) == 1
    assert df.iloc[0]["trigger_type"] == "schedule"
    assert df.iloc[0]["cron"] == "0 6 * * *"
    assert bool(df.iloc[0]["is_active"]) is True


def test_add_schedule_trigger_missing_cron_rejected(scratch_feeds, engine):
    at = _run()
    key_prefix = "add_it_0"
    at.selectbox(key=f"{key_prefix}_target_name").select(scratch_feeds["csv_feed"]).run()
    # cron deliberately left blank
    at.button(key=f"{key_prefix}_submit").click().run()

    assert not at.exception
    assert at.error, "Expected an error requiring cron for a schedule-type trigger"
    assert not at.success

    df = _fetch_trigger_for(engine, "feed", scratch_feeds["csv_feed"])
    assert df.empty


def test_sensor_trigger_ineligible_connector_kind_warns_and_rejects(scratch_feeds, engine):
    at = _run()
    key_prefix = "add_it_0"
    at.selectbox(key=f"{key_prefix}_target_name").select(scratch_feeds["postgres_feed"]).run()
    at.radio(key=f"{key_prefix}_trigger_type").set_value("sensor").run()

    assert at.warning, "Expected a sensor-ineligibility warning for a postgres-connector feed"

    at.button(key=f"{key_prefix}_submit").click().run()
    assert not at.exception
    assert at.error, "Expected an error rejecting the sensor-ineligible submission"
    assert not at.success

    df = _fetch_trigger_for(engine, "feed", scratch_feeds["postgres_feed"])
    assert df.empty


def test_sensor_trigger_eligible_connector_kind_round_trip(scratch_feeds, engine):
    at = _run()
    key_prefix = "add_it_0"
    at.selectbox(key=f"{key_prefix}_target_name").select(scratch_feeds["csv_feed"]).run()
    at.radio(key=f"{key_prefix}_trigger_type").set_value("sensor").run()

    assert not at.warning, "csv connector_kind should be sensor-eligible -- no warning expected"

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

    df = _fetch_trigger_for(engine, "feed", scratch_feeds["csv_feed"])
    assert len(df) == 1
    assert df.iloc[0]["trigger_type"] == "sensor"
    assert df.iloc[0]["cron"] is None


def test_sensor_trigger_rejected_for_model_target(scratch_model, engine):
    at = _run()
    key_prefix = "add_it_0"
    at.radio(key=f"{key_prefix}_controlling_object_type").set_value("model").run()
    at.selectbox(key=f"{key_prefix}_target_name").select(scratch_model).run()
    at.radio(key=f"{key_prefix}_trigger_type").set_value("sensor").run()
    at.button(key=f"{key_prefix}_submit").click().run()

    assert not at.exception
    assert at.error, "Expected an error rejecting a sensor-type trigger for a model target"
    assert any("only valid for a feed" in str(e.value) for e in at.error), (
        f"Expected the specific model-rejection message, got: {[str(e.value) for e in at.error]}"
    )
    assert not at.success

    df = _fetch_trigger_for(engine, "model", scratch_model)
    assert df.empty


def test_edit_existing_trigger_toggle_active_round_trip(scratch_feeds, engine):
    with engine.begin() as conn:
        feed_id = conn.execute(
            text("select id from data_feed where friendly_name = :n"), {"n": scratch_feeds["csv_feed"]}
        ).scalar()
        conn.execute(
            text(
                "insert into ingestion_triggers (trigger_type, cron, controlling_object_id, "
                "controlling_object_type, is_active) values ('schedule', '0 6 * * *', :fid, 'feed', true)"
            ),
            {"fid": feed_id},
        )

    at = _run()
    at.radio[0].set_value("Edit existing").run()
    trigger_selectbox = next(
        w for w in at.selectbox if any(scratch_feeds["csv_feed"] in str(o) for o in (w.options or []))
    )
    matching_option = next(o for o in trigger_selectbox.options if scratch_feeds["csv_feed"] in str(o))
    trigger_selectbox.select(matching_option).run()

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

    df = _fetch_trigger_for(engine, "feed", scratch_feeds["csv_feed"])
    assert bool(df.iloc[0]["is_active"]) is False


def test_delete_existing_trigger_round_trip(scratch_feeds, engine):
    with engine.begin() as conn:
        feed_id = conn.execute(
            text("select id from data_feed where friendly_name = :n"), {"n": scratch_feeds["csv_feed"]}
        ).scalar()
        conn.execute(
            text(
                "insert into ingestion_triggers (trigger_type, cron, controlling_object_id, "
                "controlling_object_type, is_active) values ('schedule', '0 6 * * *', :fid, 'feed', true)"
            ),
            {"fid": feed_id},
        )

    at = _run()
    at.radio[0].set_value("Delete existing").run()
    trigger_selectbox = next(
        w for w in at.selectbox if any(scratch_feeds["csv_feed"] in str(o) for o in (w.options or []))
    )
    matching_option = next(o for o in trigger_selectbox.options if scratch_feeds["csv_feed"] in str(o))
    trigger_selectbox.select(matching_option).run()
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

    df = _fetch_trigger_for(engine, "feed", scratch_feeds["csv_feed"])
    assert df.empty
