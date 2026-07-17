"""Real end-to-end test for `financial_transactions_sensor`: starts the
sensor for real (it's `DefaultSensorStatus.STOPPED` by default -- see that
sensor's own docstring), drops a genuine CSV file into the landing
directory, waits for the daemon's own sensor-tick loop to actually detect
it and launch `master_pipeline`, waits for that run to complete, then
always stops the sensor again afterward (regardless of outcome) so it
doesn't keep ticking for unrelated future work -- STOPPED is this sensor's
intended rest state.

This is the "heavier tier" test_sensors.py's own module docstring points
at: that file exercises the sensor's evaluation *logic* directly
(build_sensor_context, no daemon involved); this script exercises the
*real* daemon sensor-tick -> RunRequest -> K8sRunLauncher path, closing the
gap noted in Backlog.md ("nothing in the test suite actually starts the
sensor and drops a file to confirm the sensor-triggered path works end to
end").

`DagsterGraphQLClient` has no built-in start/stop-sensor or run-listing
methods (confirmed via `dir()` against the installed version) -- this uses
its own `_execute()` (the same private method its own public methods all
call internally) directly for the handful of raw GraphQL queries/mutations
those methods don't cover.

Requires dagster dev's/the in-cluster webserver already running at
localhost:3000 (same precondition as trigger_master_pipeline.py).
"""

import os
import time
from datetime import datetime, timezone

from dagster_graphql import DagsterGraphQLClient

REPOSITORY_LOCATION_NAME = "dagster_data_platform"
REPOSITORY_NAME = "__repository__"
SENSOR_NAME = "financial_transactions_sensor"

_SENSOR_SELECTOR = {
    "repositoryLocationName": REPOSITORY_LOCATION_NAME,
    "repositoryName": REPOSITORY_NAME,
    "sensorName": SENSOR_NAME,
}

_START_SENSOR_MUTATION = """
mutation($sensorSelector: SensorSelector!) {
  startSensor(sensorSelector: $sensorSelector) {
    ... on Sensor { name }
    ... on PythonError { message }
    ... on UnauthorizedError { message }
  }
}
"""

_SENSOR_STATE_ID_QUERY = """
query($sensorSelector: SensorSelector!) {
  sensorOrError(sensorSelector: $sensorSelector) {
    ... on Sensor { sensorState { id } }
    ... on PythonError { message }
  }
}
"""

_STOP_SENSOR_MUTATION = """
mutation($id: String!) {
  stopSensor(id: $id) {
    ... on StopSensorMutationResult { instigationState { id } }
    ... on PythonError { message }
    ... on UnauthorizedError { message }
  }
}
"""

_RUNS_AFTER_QUERY = """
query($pipelineName: String!, $createdAfter: Float!) {
  pipelineRunsOrError(filter: {pipelineName: $pipelineName, createdAfter: $createdAfter}) {
    ... on PipelineRuns { results { runId } }
    ... on PythonError { message }
  }
}
"""


def _client() -> DagsterGraphQLClient:
    host = os.environ.get("DAGSTER_WEBSERVER_HOST", "localhost")
    port = int(os.environ.get("DAGSTER_WEBSERVER_PORT", "3000"))
    return DagsterGraphQLClient(host, port_number=port)


def _sensor_state_id(client: DagsterGraphQLClient) -> str:
    result = client._execute(_SENSOR_STATE_ID_QUERY, {"sensorSelector": _SENSOR_SELECTOR})
    sensor_or_error = result["sensorOrError"]
    if "message" in sensor_or_error:
        raise RuntimeError(f"Could not resolve sensor state: {sensor_or_error['message']}")
    return sensor_or_error["sensorState"]["id"]


def start_sensor(client: DagsterGraphQLClient) -> None:
    result = client._execute(_START_SENSOR_MUTATION, {"sensorSelector": _SENSOR_SELECTOR})
    start_sensor_result = result["startSensor"]
    if "message" in start_sensor_result:
        raise RuntimeError(f"Failed to start sensor: {start_sensor_result['message']}")


def stop_sensor(client: DagsterGraphQLClient) -> None:
    sensor_state_id = _sensor_state_id(client)
    result = client._execute(_STOP_SENSOR_MUTATION, {"id": sensor_state_id})
    stop_sensor_result = result["stopSensor"]
    if "message" in stop_sensor_result:
        raise RuntimeError(f"Failed to stop sensor: {stop_sensor_result['message']}")


def find_run_created_after(client: DagsterGraphQLClient, *, job_name: str, created_after: float) -> str | None:
    result = client._execute(_RUNS_AFTER_QUERY, {"pipelineName": job_name, "createdAfter": created_after})
    runs_or_error = result["pipelineRunsOrError"]
    if "message" in runs_or_error:
        raise RuntimeError(f"Could not list runs: {runs_or_error['message']}")
    results = runs_or_error["results"]
    return results[0]["runId"] if results else None


def main() -> None:
    from dagster import DagsterRunStatus

    from dagster_data_platform.assets.financial_assets import _landing_dir

    client = _client()
    landing_dir = _landing_dir()
    landing_dir.mkdir(parents=True, exist_ok=True)
    # A fixed filename would collide with the sensor's own persisted
    # cursor (dagster_db, not reset between invocations of this script) --
    # confirmed live: a second run reusing the same name compared equal to
    # the cursor and was correctly skipped as "already seen", never firing
    # a RunRequest at all. The real filename convention already embeds a
    # timestamp (transactions_<YYYYMMDD_HHMMSS>.csv, see
    # generate_financial_reports.py) -- reusing it here both looks
    # realistic and guarantees a fresh name every run.
    test_filename = f"transactions_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.csv"
    test_file = landing_dir / test_filename
    # Header-only, zero data rows -- CSVConnector.fetch() (processing/connectors/connectors/csv.py)
    # uses pl.read_csv(), which raises NoDataError on a genuinely empty
    # (0-byte) file, confirmed live the hard way. A header-only file
    # parses to a valid zero-row DataFrame instead, which is all this
    # script needs (only the sensor's file-presence detection and the
    # rest of the pipeline's own "empty is fine" handling are being
    # exercised here, not real transaction data).
    test_file.write_text(
        "transaction_id,posted_date,account_code,account_name,description,debit_amount,credit_amount,currency,cost_center\n"
    )

    try:
        start_ts = time.time()
        start_sensor(client)
        print("Sensor started, waiting for a tick to pick up the new landing file...")

        run_id = None
        # minimum_interval_seconds=30 on the sensor itself -- generous
        # bound above that for the daemon to actually schedule + evaluate
        # a tick.
        for _ in range(24):
            run_id = find_run_created_after(client, job_name="master_pipeline", created_after=start_ts)
            if run_id:
                break
            time.sleep(5)
        if not run_id:
            raise RuntimeError("No master_pipeline run appeared within 120s of starting the sensor")

        print(f"Sensor-triggered master_pipeline run found: {run_id} -- waiting for it to finish...")
        deadline = time.time() + 1800
        status = client.get_run_status(run_id)
        while status not in (
            DagsterRunStatus.SUCCESS,
            DagsterRunStatus.FAILURE,
            DagsterRunStatus.CANCELED,
        ):
            if time.time() > deadline:
                raise RuntimeError(f"Run {run_id} did not finish within 1800s (last status: {status.value})")
            time.sleep(5)
            status = client.get_run_status(run_id)

        if status != DagsterRunStatus.SUCCESS:
            raise RuntimeError(f"Sensor-triggered run {run_id} finished with status {status.value}")

        print(run_id)
    finally:
        stop_sensor(client)
        test_file.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
