"""Fast, in-process tests for the generated pipeline foundation
(pipeline_generated.py) -- no cron wait, no k8s Job, no cluster launch.
Schedule evaluation logic is exercised directly via a real
ScheduleEvaluationContext (dagster.build_schedule_context), the same
mechanism trigger_schedule_run.py uses for a real end-to-end trigger (see
orchestration/module.just's verify-schedule recipe for that heavier tier).

Every generated schedule now targets the single `master_pipeline` job
(Roadmap.md "Master pipeline orchestration") -- these tests assert on the
RunRequest's orchestration_kind/orchestration_value, not on which job it
targets (there's only one).

Requires a live platform_metadata Postgres (same DB `data_feed`/
`ingestion_triggers`/`lakehouse_models` state these tests assert against) and DAGSTER_HOME set
(same as any other orchestration::test run) -- skipped, not failed, if
Postgres isn't reachable within 2s, so a fully offline `pytest` invocation
(no cluster up) doesn't error out here the way test_dbt_assets.py's pure
in-memory tests don't need one either.
"""

import os

import psycopg
import pytest
from dagster import DagsterInstance, RunRequest, SkipReason, build_schedule_context

from dagster_data_platform.pipeline_generated import ALL_SCHEDULES, EXTRACTION_JOBS
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource
from dagster_data_platform.trigger_schedule_run import find_schedule_for_feed

CONN_KWARGS = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    user=os.environ.get("POSTGRES_USER", "platform"),
    password=os.environ.get("POSTGRES_PASSWORD", "platform"),
    dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
)


def _postgres_reachable() -> bool:
    try:
        with psycopg.connect(**CONN_KWARGS, connect_timeout=2):
            return True
    except psycopg.OperationalError:
        return False


pytestmark = pytest.mark.skipif(not _postgres_reachable(), reason="platform_metadata Postgres not reachable")


def _postgres_metadata() -> PostgresMetadataResource:
    return PostgresMetadataResource(**CONN_KWARGS)


def _schedule_context():
    return build_schedule_context(
        instance=DagsterInstance.get(),
        resources={"postgres_metadata": _postgres_metadata()},
    )


def _fetch_schedule_id(cur, *, controlling_object_type: str, friendly_name: str) -> str:
    table = "data_feed" if controlling_object_type == "feed" else "lakehouse_models"
    cur.execute(
        f"""
        SELECT s.id::text FROM ingestion_triggers s
        JOIN {table} t ON t.id = s.controlling_object_id
        WHERE s.controlling_object_type = %s AND s.trigger_type = 'schedule' AND t.friendly_name = %s
        """,
        (controlling_object_type, friendly_name),
    )
    row = cur.fetchone()
    assert row is not None, f"no schedule-type ingestion_triggers row for {controlling_object_type}={friendly_name!r} -- did seeding run?"
    return row[0]


def test_every_active_feed_has_an_extraction_job():
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        cur.execute("SELECT friendly_name FROM data_feed WHERE is_active = true")
        active_feeds = {row[0] for row in cur.fetchall()}
    assert set(EXTRACTION_JOBS.keys()) == active_feeds


def test_feed_schedule_run_request_targets_master_pipeline_with_batch_group():
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        expected_schedule_id = _fetch_schedule_id(cur, controlling_object_type="feed", friendly_name="police_crimes")
        cur.execute("SELECT batch_group_friendly_name FROM data_feed WHERE friendly_name = 'police_crimes'")
        expected_batch_group = cur.fetchone()[0]

    schedule_def = find_schedule_for_feed("police_crimes", _postgres_metadata())
    result = schedule_def(_schedule_context())

    assert isinstance(result, RunRequest)
    assert result.tags["schedule_id"] == expected_schedule_id
    assert result.tags["orchestration_kind"] == "batch_group"
    assert result.tags["orchestration_value"] == expected_batch_group
    op_config = result.run_config["ops"]["run_master_pipeline"]["config"]
    assert op_config == {"orchestration_kind": "batch_group", "orchestration_value": expected_batch_group}


def test_model_schedule_run_request_targets_master_pipeline_with_model_schema():
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        schedule_id = _fetch_schedule_id(
            cur, controlling_object_type="model", friendly_name="fct_daily_financial_activity"
        )
        cur.execute(
            "SELECT model_schema FROM lakehouse_models WHERE friendly_name = 'fct_daily_financial_activity'"
        )
        expected_model_schema = cur.fetchone()[0]

    # Exactly one generated schedule per schedule row now -- no more
    # per-dependent-feed expansion (Roadmap.md "Master pipeline
    # orchestration": master_pipeline reverse-engineers depends_on_feeds
    # itself, live, from orchestration_value alone).
    matching = [s for s in ALL_SCHEDULES if s.name == f"schedule_{schedule_id.replace('-', '')}"]
    assert len(matching) == 1, f"expected exactly 1 generated schedule for schedule_id={schedule_id!r}"

    result = matching[0](_schedule_context())
    assert isinstance(result, RunRequest)
    assert result.tags["schedule_id"] == schedule_id
    assert result.tags["orchestration_kind"] == "model_schema"
    assert result.tags["orchestration_value"] == expected_model_schema


def test_schedule_skips_when_inactive():
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        schedule_id = _fetch_schedule_id(cur, controlling_object_type="feed", friendly_name="police_crimes")

    schedule_def = find_schedule_for_feed("police_crimes", _postgres_metadata())
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        cur.execute("UPDATE ingestion_triggers SET is_active = false WHERE id = %s", (schedule_id,))
        conn.commit()
    try:
        result = schedule_def(_schedule_context())
        assert isinstance(result, SkipReason)
    finally:
        with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
            cur.execute("UPDATE ingestion_triggers SET is_active = true WHERE id = %s", (schedule_id,))
            conn.commit()
