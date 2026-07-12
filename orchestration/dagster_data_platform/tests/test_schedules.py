"""Fast, in-process tests for the generated pipeline foundation
(pipeline_generated.py) -- no cron wait, no k8s Job, no cluster launch.
Schedule evaluation logic is exercised directly via a real
ScheduleEvaluationContext (dagster.build_schedule_context), the same
mechanism trigger_schedule_run.py uses for a real end-to-end trigger (see
orchestration/module.just's verify-schedule recipe for that heavier tier).

Requires a live platform_metadata Postgres (same DB `data_feed`/`schedule`/
`lakehouse_models` state these tests assert against) and DAGSTER_HOME set
(same as any other orchestration::test run) -- skipped, not failed, if
Postgres isn't reachable within 2s, so a fully offline `pytest` invocation
(no cluster up) doesn't error out here the way test_dbt_assets.py's pure
in-memory tests don't need one either.
"""

import os

import psycopg
import pytest
from dagster import DagsterInstance, RunRequest, SkipReason, build_schedule_context

from dagster_data_platform.pipeline_generated import ALL_SCHEDULES, FEED_JOBS
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


def _schedule_context():
    return build_schedule_context(
        instance=DagsterInstance.get(),
        resources={"postgres_metadata": PostgresMetadataResource(**CONN_KWARGS)},
    )


def _fetch_schedule_id(cur, *, controlling_object_type: str, friendly_name: str) -> str:
    table = "data_feed" if controlling_object_type == "feed" else "lakehouse_models"
    cur.execute(
        f"""
        SELECT s.id::text FROM schedule s
        JOIN {table} t ON t.id = s.controlling_object_id
        WHERE s.controlling_object_type = %s AND t.friendly_name = %s
        """,
        (controlling_object_type, friendly_name),
    )
    row = cur.fetchone()
    assert row is not None, f"no schedule row for {controlling_object_type}={friendly_name!r} -- did seeding run?"
    return row[0]


def test_every_active_feed_has_a_job():
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        cur.execute("SELECT friendly_name FROM data_feed WHERE is_active = true")
        active_feeds = {row[0] for row in cur.fetchall()}
    assert set(FEED_JOBS.keys()) == active_feeds


def test_feed_schedule_run_request_has_correct_parameters():
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        expected_schedule_id = _fetch_schedule_id(cur, controlling_object_type="feed", friendly_name="police_crimes")

    schedule_def = find_schedule_for_feed("police_crimes")
    result = schedule_def(_schedule_context())

    assert isinstance(result, RunRequest)
    assert result.tags["schedule_id"] == expected_schedule_id
    assert result.tags["controlling_object_type"] == "feed"
    assert result.tags["controlling_object_friendly_name"] == "police_crimes"
    assert result.tags["feed_friendly_name"] == "police_crimes"
    assert result.tags["batch_group_friendly_name"]  # non-empty, real value looked up live


def test_model_schedule_expands_to_one_run_request_per_dependent_feed():
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        schedule_id = _fetch_schedule_id(
            cur, controlling_object_type="model", friendly_name="fct_daily_financial_activity"
        )

    matching = [s for s in ALL_SCHEDULES if s.name.startswith(f"schedule_{schedule_id.replace('-', '')}_")]
    assert len(matching) == 2, f"expected 2 generated schedules (sales, financial_transactions), got {len(matching)}"

    feeds_seen = set()
    for schedule_def in matching:
        result = schedule_def(_schedule_context())
        assert isinstance(result, RunRequest)
        assert result.tags["schedule_id"] == schedule_id
        assert result.tags["controlling_object_type"] == "model"
        assert result.tags["controlling_object_friendly_name"] == "fct_daily_financial_activity"
        assert schedule_def.job_name == FEED_JOBS[result.tags["feed_friendly_name"]].name
        feeds_seen.add(result.tags["feed_friendly_name"])

    assert feeds_seen == {"sales", "financial_transactions"}


def test_schedule_skips_when_inactive():
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        schedule_id = _fetch_schedule_id(cur, controlling_object_type="feed", friendly_name="police_crimes")

    schedule_def = find_schedule_for_feed("police_crimes")
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        cur.execute("UPDATE schedule SET is_active = false WHERE id = %s", (schedule_id,))
        conn.commit()
    try:
        result = schedule_def(_schedule_context())
        assert isinstance(result, SkipReason)
    finally:
        with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
            cur.execute("UPDATE schedule SET is_active = true WHERE id = %s", (schedule_id,))
            conn.commit()
