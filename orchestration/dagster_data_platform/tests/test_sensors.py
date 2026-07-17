"""Fast, in-process tests for a generated per-feed sensor (see
scripts/generate_dagster_pipeline.py's `_make_master_pipeline_sensor`) --
exercises the sensor's own evaluation logic directly via a real
SensorEvaluationContext (dagster.build_sensor_context, the sensor
equivalent of test_schedules.py's build_schedule_context), no cron/interval
wait, no live cluster launch. Tests generically against `ALL_SENSORS`/
`SENSOR_ORCHESTRATION` (mirroring test_schedules.py's own `ALL_SCHEDULES`
pattern) rather than a hardcoded sensor object, since Item 2+3's
ingestion_triggers generalization means any csv/json_file-connector feed
can have a generated sensor now, not just financial_transactions (the
original, still-default, proof-of-concept these tests exercise).

The original hand-written financial_transactions_sensor this replaced was
found bypassing the master pipeline entirely earlier in this project's
history (see Learnings.md, "A feed whose source is the pipeline's own run
history..." fourth occurrence, and Backlog.md's "verify-pipeline/smoketest
rework must also exercise the sensor-triggered path") -- that bug survived
undetected specifically because nothing in the test suite ever evaluated
the sensor at all (every generated sensor is DefaultSensorStatus.STOPPED
by default, and neither `just smoketest` nor `verify-pipeline`/
`verify-schedule` ever turns one on). These tests close that gap at the
logic level; a real end-to-end run (starting the sensor, dropping a file,
waiting for the daemon to actually launch and complete a run) is a
heavier, separate tier -- see `orchestration/module.just`'s `verify-sensor`
recipe for that.

Requires a live platform_metadata Postgres (same DB `data_feed` state these
tests assert against) -- skipped, not failed, if unreachable, same pattern
as test_schedules.py.
"""

import os

import psycopg
import pytest
from dagster import DagsterInstance, RunRequest, build_sensor_context

from dagster_data_platform.assets.financial_assets import _landing_dir
from dagster_data_platform.pipeline_generated import ALL_SENSORS, SENSOR_ORCHESTRATION
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource

FEED_FRIENDLY_NAME = "financial_transactions"

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


def _sensor_context(cursor: str | None = None):
    return build_sensor_context(
        instance=DagsterInstance.get(),
        cursor=cursor,
        resources={"postgres_metadata": _postgres_metadata()},
    )


def _find_sensor(feed_friendly_name: str):
    for sensor_def in ALL_SENSORS:
        target_feed, _orchestration_value = SENSOR_ORCHESTRATION.get(sensor_def.name, (None, None))
        if target_feed == feed_friendly_name:
            return sensor_def
    raise ValueError(f"No generated sensor targets feed {feed_friendly_name!r} -- did seeding run?")


@pytest.fixture
def throwaway_landing_file():
    """A uniquely-named, empty CSV dropped into the real landing directory
    -- the sensor only ever checks file presence/name, never content, so
    an empty file is sufficient. Named to sort after any real file already
    there (a fixed far-future timestamp), and removed afterward regardless
    of test outcome -- same "throwaway, cleaned up after" convention as
    this project's other live-data tests (see Progress.md's scratch-test
    entries)."""
    landing_dir = _landing_dir()
    landing_dir.mkdir(parents=True, exist_ok=True)
    filename = "transactions_99991231_235959.csv"
    path = landing_dir / filename
    path.write_text("")
    try:
        yield filename
    finally:
        path.unlink(missing_ok=True)


def test_no_run_request_when_landing_is_empty(tmp_path, monkeypatch):
    # Points DATA_LAKE_PATH at a genuinely empty scratch directory (not the
    # real data-lake/, which may have real files from other tests/runs) --
    # confirms the sensor is a true no-op, not just "didn't error", when
    # there's nothing new to see.
    monkeypatch.setenv("DATA_LAKE_PATH", str(tmp_path))
    sensor_def = _find_sensor(FEED_FRIENDLY_NAME)
    context = _sensor_context()
    result = sensor_def(context)
    assert result is None


def test_new_landing_file_yields_run_request_targeting_master_pipeline(throwaway_landing_file):
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        cur.execute("SELECT batch_group_friendly_name FROM data_feed WHERE friendly_name = 'financial_transactions'")
        expected_batch_group = cur.fetchone()[0]

    sensor_def = _find_sensor(FEED_FRIENDLY_NAME)
    context = _sensor_context()
    request = sensor_def(context)

    assert isinstance(request, RunRequest)
    assert request.run_key == throwaway_landing_file
    op_config = request.run_config["ops"]["run_master_pipeline"]["config"]
    assert op_config == {"orchestration_kind": "batch_group", "orchestration_value": expected_batch_group}


def test_cursor_already_at_latest_file_yields_nothing(throwaway_landing_file):
    # Simulates "the sensor already saw this file on a previous tick" --
    # the exact case the cursor exists to prevent duplicate RunRequests for.
    sensor_def = _find_sensor(FEED_FRIENDLY_NAME)
    context = _sensor_context(cursor=throwaway_landing_file)
    result = sensor_def(context)
    assert result is None
