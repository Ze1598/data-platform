"""Tests for wake_sleep_sensor.py -- the sleep side of the cooperative
wake-up mechanism (Backlog.md, "Cooperative wake-up mechanism for Dagster
from Streamlit"; see frontend/dagster_wake.py for the wake side).

`_sleep_if_no_other_runs_in_flight` is tested directly against a real,
ephemeral `DagsterInstance` (dagster._core.test_utils.instance_for_test,
runs seeded via create_run_for_test) rather than via
`build_run_status_sensor_context` -- that helper needs a real DagsterEvent
+ DagsterRun from an actually-executed job, and the function under test
here only ever reads `context.instance`/`context.log`, so a lightweight
fake context (this file's `_FakeContext`) exercises the real logic
without that machinery. The `kubernetes` client is mocked throughout --
no live cluster needed.

Whether each of the three sensors actually only fires for `master_pipeline`
(not every individual child job -- extraction/modeling/serving jobs are
separate `@job`s) is `monitored_jobs` filtering, which happens in
Dagster's own sensor-evaluation/daemon layer, not inside the decorated
function itself -- so it can't be proven by invoking the function with a
different job's event (the function has no idea what job the event was
for). It's checked declaratively instead, against the constructed
`RunStatusSensorDefinition`'s own `_monitored_jobs`.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from dagster import DagsterRunStatus
from dagster._core.test_utils import create_run_for_test, instance_for_test

from dagster_data_platform.pipeline_generated import master_pipeline
from dagster_data_platform.wake_sleep_sensor import (
    ALL_WAKE_SLEEP_SENSORS,
    _recently_woken,
    _sleep_if_no_other_runs_in_flight,
)


def _fake_context(instance):
    return SimpleNamespace(instance=instance, log=MagicMock())


def _scaled_object_response(wake_timestamp: str | None = None) -> dict:
    """Shape of what CustomObjectsApi.get_namespaced_custom_object
    actually returns for a ScaledObject -- a plain dict (the k8s Python
    client returns raw dicts for CustomObjectsApi calls, not typed
    model objects, unlike AppsV1Api/CoreV1Api)."""
    annotations = {}
    if wake_timestamp is not None:
        annotations["data-platform.internal/last-woken-at"] = wake_timestamp
    return {"metadata": {"annotations": annotations}}


# Default response for every test below that doesn't care about the wake
# grace period itself -- no wake-timestamp annotation, so _recently_woken
# is always False and the original (pre-grace-period) behavior holds.
_NOT_RECENTLY_WOKEN = _scaled_object_response()


def test_all_sensors_are_scoped_to_master_pipeline_only():
    # Declarative check, not a live invocation -- see module docstring for
    # why. If this ever regresses to monitoring every job in the
    # workspace, the sensor would fire (and try to unpause orchestration)
    # on every individual extraction/modeling/serving child job
    # completing mid-run, not just on master_pipeline's own terminal
    # status.
    for sensor_def in ALL_WAKE_SLEEP_SENSORS:
        assert sensor_def._monitored_jobs == [master_pipeline], (
            f"{sensor_def.name} is not scoped to exactly [master_pipeline]: {sensor_def._monitored_jobs}"
        )


def test_all_sensors_default_to_running():
    # Unlike pipeline_generated.py's generated schedules/sensors (STOPPED
    # by default, a per-trigger opt-in) -- this is infrastructure
    # housekeeping that must be active from first deploy, or a paused
    # ScaledObject would never get released at all.
    for sensor_def in ALL_WAKE_SLEEP_SENSORS:
        assert sensor_def.default_status.value == "RUNNING", f"{sensor_def.name} does not default to RUNNING"


def test_all_three_terminal_statuses_covered():
    # `_run_status` (private) is the only accessor for this -- no public
    # equivalent exists on RunStatusSensorDefinition.
    covered = {s._run_status for s in ALL_WAKE_SLEEP_SENSORS}
    assert covered == {DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED}


@patch("dagster_data_platform.wake_sleep_sensor.config")
@patch("dagster_data_platform.wake_sleep_sensor.client")
def test_unpauses_when_no_other_master_pipeline_runs_in_flight(mock_client, mock_config):
    custom_api = MagicMock()
    custom_api.get_namespaced_custom_object.return_value = _NOT_RECENTLY_WOKEN
    mock_client.CustomObjectsApi.return_value = custom_api

    with instance_for_test() as instance:
        # The just-completed run itself is already terminal (SUCCESS) by
        # the time the sensor fires -- only non-terminal runs should block
        # unpausing, so seed just that one, terminal, run.
        create_run_for_test(instance, job_name="master_pipeline", status=DagsterRunStatus.SUCCESS)

        _sleep_if_no_other_runs_in_flight(_fake_context(instance))

    assert custom_api.patch_namespaced_custom_object.call_count == 2
    called_names = set()
    for call in custom_api.patch_namespaced_custom_object.call_args_list:
        kwargs = call.kwargs
        assert kwargs["group"] == "keda.sh"
        assert kwargs["namespace"] == "orchestration"
        assert kwargs["plural"] == "scaledobjects"
        assert kwargs["body"] == {"metadata": {"annotations": {"autoscaling.keda.sh/paused-replicas": None}}}
        called_names.add(kwargs["name"])
    assert called_names == {"dagster-webserver-scaler", "dagster-code-server-scaler"}


@patch("dagster_data_platform.wake_sleep_sensor.config")
@patch("dagster_data_platform.wake_sleep_sensor.client")
def test_leaves_paused_when_another_master_pipeline_run_is_in_flight(mock_client, mock_config):
    custom_api = MagicMock()
    mock_client.CustomObjectsApi.return_value = custom_api

    with instance_for_test() as instance:
        create_run_for_test(instance, job_name="master_pipeline", status=DagsterRunStatus.SUCCESS)
        # A second, still-running master_pipeline invocation (e.g. another
        # Streamlit user, or an overlapping cron-scheduled trigger).
        create_run_for_test(instance, job_name="master_pipeline", status=DagsterRunStatus.STARTED)

        _sleep_if_no_other_runs_in_flight(_fake_context(instance))

    custom_api.patch_namespaced_custom_object.assert_not_called()


@patch("dagster_data_platform.wake_sleep_sensor.config")
@patch("dagster_data_platform.wake_sleep_sensor.client")
def test_k8s_client_exception_is_swallowed_not_raised(mock_client, mock_config):
    # Best-effort, matching _sleep-orchestration's own `|| true` shell
    # convention -- a failed unpause just means orchestration stays awake
    # a bit longer, not a broken run or a crashed sensor tick.
    custom_api = MagicMock()
    custom_api.get_namespaced_custom_object.return_value = _NOT_RECENTLY_WOKEN
    custom_api.patch_namespaced_custom_object.side_effect = Exception("boom")
    mock_client.CustomObjectsApi.return_value = custom_api

    with instance_for_test() as instance:
        create_run_for_test(instance, job_name="master_pipeline", status=DagsterRunStatus.SUCCESS)
        context = _fake_context(instance)
        _sleep_if_no_other_runs_in_flight(context)  # must not raise

    context.log.warning.assert_called()


@patch("dagster_data_platform.wake_sleep_sensor.config")
@patch("dagster_data_platform.wake_sleep_sensor.client")
def test_leaves_paused_when_recently_woken_even_with_no_runs_in_flight(mock_client, mock_config):
    # The actual race this closes (found live, 2026-07-20): a stale
    # sleep-sensor tick for an unrelated older run can fire in the exact
    # window after a fresh wake, before the new trigger's own run has been
    # created -- "no other runs in flight" alone would incorrectly say
    # it's safe to unpause.
    custom_api = MagicMock()
    just_now = datetime.now(timezone.utc).isoformat()
    custom_api.get_namespaced_custom_object.return_value = _scaled_object_response(just_now)
    mock_client.CustomObjectsApi.return_value = custom_api

    with instance_for_test() as instance:
        create_run_for_test(instance, job_name="master_pipeline", status=DagsterRunStatus.SUCCESS)
        context = _fake_context(instance)
        _sleep_if_no_other_runs_in_flight(context)

    custom_api.patch_namespaced_custom_object.assert_not_called()
    context.log.info.assert_called()


@patch("dagster_data_platform.wake_sleep_sensor.config")
@patch("dagster_data_platform.wake_sleep_sensor.client")
def test_unpauses_when_wake_timestamp_is_stale(mock_client, mock_config):
    custom_api = MagicMock()
    long_ago = (datetime.now(timezone.utc) - timedelta(seconds=300)).isoformat()
    custom_api.get_namespaced_custom_object.return_value = _scaled_object_response(long_ago)
    mock_client.CustomObjectsApi.return_value = custom_api

    with instance_for_test() as instance:
        create_run_for_test(instance, job_name="master_pipeline", status=DagsterRunStatus.SUCCESS)
        _sleep_if_no_other_runs_in_flight(_fake_context(instance))

    assert custom_api.patch_namespaced_custom_object.call_count == 2


def test_recently_woken_true_for_a_fresh_timestamp():
    custom_api = MagicMock()
    custom_api.get_namespaced_custom_object.return_value = _scaled_object_response(
        datetime.now(timezone.utc).isoformat()
    )
    assert _recently_woken(custom_api) is True


def test_recently_woken_false_for_a_stale_timestamp():
    custom_api = MagicMock()
    stale = (datetime.now(timezone.utc) - timedelta(seconds=61)).isoformat()
    custom_api.get_namespaced_custom_object.return_value = _scaled_object_response(stale)
    assert _recently_woken(custom_api) is False


def test_recently_woken_false_when_annotation_missing():
    custom_api = MagicMock()
    custom_api.get_namespaced_custom_object.return_value = _scaled_object_response()
    assert _recently_woken(custom_api) is False


def test_recently_woken_false_on_malformed_timestamp():
    custom_api = MagicMock()
    custom_api.get_namespaced_custom_object.return_value = _scaled_object_response("not-a-timestamp")
    assert _recently_woken(custom_api) is False


def test_recently_woken_false_when_k8s_api_call_fails():
    custom_api = MagicMock()
    custom_api.get_namespaced_custom_object.side_effect = Exception("boom")
    assert _recently_woken(custom_api) is False


# No test directly invokes master_pipeline_sleep_on_success/_failure/
# _canceled themselves -- a decorated RunStatusSensorDefinition's __call__
# type-checks its argument as a real RunStatusSensorContext (constructible
# only via build_run_status_sensor_context, which itself needs a genuine
# DagsterEvent from an actually-executed job), rejecting a lightweight
# fake. All three are one-line delegations to
# _sleep_if_no_other_runs_in_flight (visible directly in
# wake_sleep_sensor.py), which is exercised thoroughly above; a live
# end-to-end run (this project's `just orchestration::verify-pipeline`
# tier) is what actually proves the full decorated sensors fire correctly.
