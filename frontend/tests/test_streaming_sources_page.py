"""Exercises frontend/pages/5_Streaming_Sources.py through Streamlit's own
headless script-running harness (AppTest) -- same philosophy as
test_trigger_pipeline_page.py: real widget interaction plus a live-Postgres
(and, for the "Discover schema" tests, live-Kafka) round trip check, not
just "no exception raised."

This page had zero test coverage of any kind before this file.

The "Discover schema now" tests below are also a direct regression check on
the 2026-07-27 renumbering fix itself (Progress.md, "Documentation-vs-
implementation audit"): this page's own code hardcodes a self-referential
`created_by="5_Streaming_Sources_discover_schema"` tag (metadata_db.
write_schema_registry_version's audit column) -- if the rename had been done
sloppily this literal could silently still say "4_Streaming_Sources...",
compiling fine (it's just a string) but writing a wrong audit tag forever.
test_discover_schema_writes_correct_created_by_tag asserts the *actual*
value landed in schema_registry, not just that the page loads.

Needs the live cluster reachable (same as test_metadata_db.py). The
"Discover schema" tests additionally need a real Kafka topic with messages
actually flowing -- uses the real, always-seeded `sales_events` streaming
source, backed by the continuously-running `sales-events-producer`
Deployment (streaming/producer/), rather than a scratch topic nothing ever
writes to.
"""

import socket
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import text
from streamlit.testing.v1 import AppTest

from metadata_db import delete_row, fetch_current_schema, get_engine, write_schema_registry_version

_PAGE_PATH = Path(__file__).resolve().parents[1] / "pages" / "5_Streaming_Sources.py"
_TEST_PREFIX = "zz_apptest_stream_"
_REAL_DISCOVERY_SOURCE = "sales_events"
_EXPECTED_CREATED_BY = f"{_PAGE_PATH.stem}_discover_schema"
_KAFKA_HOST = "kafka.streaming.svc.cluster.local"


def _kafka_dns_resolvable() -> bool:
    """The page hardcodes this in-cluster Service DNS name -- it only
    resolves from inside the cluster's own pod network (where the real
    frontend pod runs), never from a host-side pytest invocation. Same
    constraint this repo's streaming/testing/ module is already built
    around (Kafka's Service is ClusterIP-only, see Learnings.md) -- skip
    cleanly here rather than let the test hang on a DNS timeout."""
    try:
        socket.getaddrinfo(_KAFKA_HOST, 9092)
        return True
    except socket.gaierror:
        return False


def _run():
    at = AppTest.from_file(str(_PAGE_PATH), default_timeout=30)
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
                    "delete from schema_registry where controlling_object_type = 'streaming_source' "
                    "and controlling_object_id in "
                    "(select id from streaming_source where friendly_name like :p)"
                ),
                {"p": f"{_TEST_PREFIX}%"},
            )
            conn.execute(text("delete from streaming_source where friendly_name like :p"), {"p": f"{_TEST_PREFIX}%"})

    _delete_all()
    yield
    _delete_all()


def _fetch_source(engine, friendly_name: str) -> pd.DataFrame:
    return pd.read_sql(
        text("select * from streaming_source where friendly_name = :name"), engine, params={"name": friendly_name}
    )


def test_page_loads_and_lists_streaming_sources():
    at = _run()
    assert not at.exception, f"Page load raised: {[str(e.value) for e in at.exception]}"
    assert at.dataframe, "Expected the streaming_source table to render"
    assert at.radio, "Expected the Add/Edit/Discover/Delete action radio"
    assert set(at.radio[0].options) == {"Add new", "Edit existing", "Discover schema", "Delete existing"}


def test_add_streaming_source_round_trip(engine):
    name = f"{_TEST_PREFIX}new"
    at = _run()
    key_prefix = "add_ss_0"

    at.text_input(key=f"{key_prefix}_friendly_name").set_value(name).run()
    at.text_input(key=f"{key_prefix}_topic_name").set_value(name).run()
    at.text_input(key=f"{key_prefix}_table_name").set_value(name).run()
    # Pick an existing model_schema (domain) rather than "<New domain>" --
    # simpler happy path, the new-domain slug validation gets its own test.
    at.selectbox(key=f"{key_prefix}_model_schema_choice").select("sales").run()
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

    df = _fetch_source(engine, name)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["topic_name"] == name
    assert row["table_name"] == name
    assert row["model_schema"] == "sales"
    assert bool(row["schema_discovery_enabled"]) is True, "Default should be enabled"
    assert bool(row["autoscaler_enabled"]) is False, "Default should be disabled"
    assert pd.isna(row["jobmanager_memory"]) or row["jobmanager_memory"] is None


def test_add_streaming_source_new_domain_invalid_slug_rejected(engine):
    name = f"{_TEST_PREFIX}badslug"
    at = _run()
    key_prefix = "add_ss_0"

    at.text_input(key=f"{key_prefix}_friendly_name").set_value(name).run()
    at.text_input(key=f"{key_prefix}_topic_name").set_value(name).run()
    at.text_input(key=f"{key_prefix}_table_name").set_value(name).run()
    # "<New domain>" is already the default choice -- fill an invalid slug.
    at.text_input(key=f"{key_prefix}_new_model_schema").set_value("Not Valid!").run()
    at.button(key=f"{key_prefix}_submit").click().run()

    assert not at.exception
    assert at.error, "Expected an error rejecting an invalid model_schema slug"
    assert not at.success

    df = _fetch_source(engine, name)
    assert df.empty


def test_add_streaming_source_missing_required_fields_rejected():
    at = _run()
    key_prefix = "add_ss_0"
    # Everything left blank.
    at.button(key=f"{key_prefix}_submit").click().run()

    assert not at.exception
    assert at.error, "Expected an error for missing friendly_name/topic_name/table_name"
    assert not at.success


def test_add_streaming_source_invalid_taskmanager_cpu_rejected(engine):
    name = f"{_TEST_PREFIX}badcpu"
    at = _run()
    key_prefix = "add_ss_0"

    at.text_input(key=f"{key_prefix}_friendly_name").set_value(name).run()
    at.text_input(key=f"{key_prefix}_topic_name").set_value(name).run()
    at.text_input(key=f"{key_prefix}_table_name").set_value(name).run()
    at.selectbox(key=f"{key_prefix}_model_schema_choice").select("sales").run()
    at.text_input(key=f"{key_prefix}_tm_cpu").set_value("not-a-number").run()
    at.button(key=f"{key_prefix}_submit").click().run()

    assert not at.exception
    assert at.error, "Expected an error rejecting a non-numeric TaskManager CPU"
    assert any("must be a number" in str(e.value) for e in at.error), (
        f"Expected the specific CPU-parse error, got: {[str(e.value) for e in at.error]}"
    )
    assert not at.success

    df = _fetch_source(engine, name)
    assert df.empty


def test_edit_existing_streaming_source_round_trip(engine):
    name = f"{_TEST_PREFIX}editme"
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO streaming_source (friendly_name, topic_name, table_name, model_schema, is_active)
                VALUES (:name, :name, :name, 'sales', true)
                """
            ),
            {"name": name},
        )

    at = _run()
    at.radio[0].set_value("Edit existing").run()
    source_selectbox = next(w for w in at.selectbox if name in (w.options or []))
    source_selectbox.select(name).run()

    tm_cpu_input = next(w for w in at.text_input if w.key and w.key.endswith("_tm_cpu"))
    tm_cpu_input.set_value("0.75").run()
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

    df = _fetch_source(engine, name)
    row = df.iloc[0]
    assert float(row["taskmanager_cpu"]) == 0.75
    # friendly_name/table_name are immutable once created -- confirm the
    # page's values.pop() actually protected them, not just that the edit
    # "succeeded" some other way.
    assert row["friendly_name"] == name
    assert row["table_name"] == name


def test_delete_existing_streaming_source_round_trip(engine):
    name = f"{_TEST_PREFIX}deleteme"
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO streaming_source (friendly_name, topic_name, table_name, model_schema) "
                "VALUES (:name, :name, :name, 'sales')"
            ),
            {"name": name},
        )

    at = _run()
    at.radio[0].set_value("Delete existing").run()
    source_selectbox = next(w for w in at.selectbox if name in (w.options or []))
    source_selectbox.select(name).run()
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

    df = _fetch_source(engine, name)
    assert df.empty


def test_edit_existing_source_shows_discovered_columns_and_event_timestamp_picker(engine):
    """Read-only sub-path, no Kafka involved: once a source already has a
    schema_registry row (from a prior discovery run), 'Edit existing' should
    render an 'Event timestamp column' selectbox populated from those
    discovered columns, not the 'Run schema discovery first' info message."""
    name = f"{_TEST_PREFIX}discovered"
    with engine.begin() as conn:
        source_id = conn.execute(
            text(
                "INSERT INTO streaming_source (friendly_name, topic_name, table_name, model_schema, "
                "event_timestamp_column) VALUES (:name, :name, :name, 'sales', 'event_ts') RETURNING id"
            ),
            {"name": name},
        ).scalar()
    write_schema_registry_version(
        engine,
        controlling_object_id=str(source_id),
        controlling_object_type="streaming_source",
        column_definitions=[
            {"name": "event_id", "data_type": "string", "nullable": False},
            {"name": "event_ts", "data_type": "timestamp", "nullable": False},
            {"name": "amount", "data_type": "double", "nullable": True},
        ],
        primary_key_columns=[],
        created_by="test_fixture",
    )

    at = _run()
    at.radio[0].set_value("Edit existing").run()
    source_selectbox = next(w for w in at.selectbox if name in (w.options or []))
    source_selectbox.select(name).run()

    assert not at.exception, f"Editing raised: {[str(e.value) for e in at.exception]}"
    assert not any(
        "Run schema discovery" in i.value for i in at.info
    ), "Should not prompt to run discovery when columns are already discovered"

    ts_selectbox = next(w for w in at.selectbox if w.key and w.key.endswith("_event_timestamp_column"))
    assert set(ts_selectbox.options) == {"event_id", "event_ts", "amount"}
    assert ts_selectbox.value == "event_ts", "Should default to the source's already-saved event_timestamp_column"


def test_discover_schema_mode_shows_already_discovered_columns(engine):
    """Read-only sub-path, no Kafka involved: selecting a source with an
    existing schema_registry row under 'Discover schema' mode should render
    the 'Current discovered schema' table -- this is a pure Postgres read,
    independent of whether a NEW discovery run against Kafka is ever
    triggered."""
    name = f"{_TEST_PREFIX}current_schema"
    with engine.begin() as conn:
        source_id = conn.execute(
            text(
                "INSERT INTO streaming_source (friendly_name, topic_name, table_name, model_schema) "
                "VALUES (:name, :name, :name, 'sales') RETURNING id"
            ),
            {"name": name},
        ).scalar()
    write_schema_registry_version(
        engine,
        controlling_object_id=str(source_id),
        controlling_object_type="streaming_source",
        column_definitions=[{"name": "branch", "data_type": "string", "nullable": False}],
        primary_key_columns=[],
        created_by="test_fixture",
    )

    at = _run()
    at.radio[0].set_value("Discover schema").run()
    at.selectbox(key="discover_select").select(name).run()

    assert not at.exception, f"Selecting the source raised: {[str(e.value) for e in at.exception]}"
    assert not any(
        "No schema discovered yet" in i.value for i in at.info
    ), "Should not claim no schema is discovered when one already exists"
    assert at.dataframe, "Expected the 'Current discovered schema' table to render"
    rendered_columns = {c["name"] for c in at.dataframe[-1].value.to_dict("records")}
    assert rendered_columns == {"branch"}


def test_discover_schema_writes_correct_created_by_tag(engine):
    """Live Kafka test -- needs a real topic with messages actually
    flowing. Uses the real, always-seeded sales_events source (backed by
    the continuously-running sales-events-producer Deployment), not a
    scratch topic nothing writes to. Skips cleanly if sales_events isn't
    present/active/discovery-enabled rather than failing on an environment
    precondition this test doesn't control. Also skips (rather than fails)
    when kafka.streaming.svc.cluster.local can't be resolved -- that's an
    in-cluster-only Service DNS name; this test can only exercise the live
    Kafka path when run from inside the cluster's own pod network (e.g. as
    an in-cluster Job, mirroring streaming/testing/), not from a host-side
    pytest invocation."""
    if not _kafka_dns_resolvable():
        pytest.skip(
            f"{_KAFKA_HOST!r} is not resolvable from this environment -- Kafka's Service is "
            "ClusterIP-only, so this test can only run from inside the cluster's pod network "
            "(see Learnings.md, 'Host-side tools need a NodePort...')."
        )
    source_df = pd.read_sql(
        text("select * from streaming_source where friendly_name = :name"),
        engine,
        params={"name": _REAL_DISCOVERY_SOURCE},
    )
    if source_df.empty:
        pytest.skip(f"{_REAL_DISCOVERY_SOURCE!r} streaming_source not seeded in this environment")
    if not bool(source_df.iloc[0]["schema_discovery_enabled"]):
        pytest.skip(f"{_REAL_DISCOVERY_SOURCE!r} has schema_discovery_enabled=false")

    at = _run()
    at.radio[0].set_value("Discover schema").run()
    at.selectbox(key="discover_select").select(_REAL_DISCOVERY_SOURCE).run()

    if not at.button:
        pytest.skip(f"No 'Discover schema now' button rendered for {_REAL_DISCOVERY_SOURCE!r}")

    # Bound the worst case: consuming fewer sample messages means fewer
    # poll attempts (sample_size * 5, up to 2.0s per attempt, see the
    # page's own comment) before the loop gives up.
    at.number_input[0].set_value(5).run()
    at.button[0].click().run(timeout=60)

    assert not at.exception, f"Clicking Discover schema now raised: {[str(e.value) for e in at.exception]}"
    # Not asserting at.success here -- same chained-rerun capture
    # unreliability as every other Add/Edit/Delete flow in this app (see
    # this file's other tests). at.error IS reliable (the error path never
    # reaches st.rerun()), so a real failure -- e.g. sales-events-producer
    # not actually running -- still surfaces here.
    assert not at.error, (
        f"Expected no errors -- if this fails, confirm sales-events-producer is actually running "
        f"and producing to the '{_REAL_DISCOVERY_SOURCE}' topic. Errors: {[str(e.value) for e in at.error]}"
    )

    source_id = str(source_df.iloc[0]["id"])
    columns = fetch_current_schema(engine, source_id, "streaming_source")
    assert columns, "Expected fetch_current_schema to return real discovered columns"

    tag_df = pd.read_sql(
        text(
            "select created_by from schema_registry where controlling_object_id = :id "
            "and controlling_object_type = 'streaming_source' and is_current"
        ),
        engine,
        params={"id": source_id},
    )
    assert not tag_df.empty
    assert tag_df.iloc[0]["created_by"] == _EXPECTED_CREATED_BY, (
        f"created_by tag should match this page's current filename-derived value "
        f"({_EXPECTED_CREATED_BY!r}) -- got {tag_df.iloc[0]['created_by']!r}. A mismatch here means the "
        "2026-07-27 renumbering left a stale hardcoded page-name literal in the page source."
    )
