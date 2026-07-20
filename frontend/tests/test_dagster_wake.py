"""Unit-level tests for dagster_wake.py -- mocks the `kubernetes` client
entirely, no live cluster needed (the live end-to-end proof that this
actually wakes a real scaled-to-zero orchestration lives in
test_trigger_pipeline_page.py's force_cold_start-based test instead).

First k8s-mocking pattern in this repo's tests -- every other test file
(test_metadata_db.py, test_trigger_pipeline_page.py) is a live-Postgres/
live-cluster integration test by design. This one is deliberately unit-
level: exercising wake_orchestration()'s timeout/retry edge cases against
a real cluster would be slow and flaky (waiting out a real 120s timeout).
"""

from unittest.mock import MagicMock, patch

import pytest

import dagster_wake


@pytest.fixture(autouse=True)
def _reset_config_cache():
    # wake_orchestration() only calls kubernetes.config.load_*_config()
    # once per process (module-level _config_loaded flag) -- reset it
    # between tests so each test's mocked config.load_kube_config() call
    # is actually exercised, not skipped by a prior test's cached state.
    dagster_wake._config_loaded = False
    yield
    dagster_wake._config_loaded = False


def _ready_deployment(ready_replicas: int):
    dep = MagicMock()
    dep.status.ready_replicas = ready_replicas
    return dep


@patch("dagster_wake.config")
@patch("dagster_wake.client")
def test_wake_orchestration_patches_both_scaled_objects(mock_client, mock_config):
    custom_api = MagicMock()
    apps_api = MagicMock()
    mock_client.CustomObjectsApi.return_value = custom_api
    mock_client.AppsV1Api.return_value = apps_api
    apps_api.read_namespaced_deployment.return_value = _ready_deployment(1)

    dagster_wake.wake_orchestration()

    assert custom_api.patch_namespaced_custom_object.call_count == 2
    called_names = set()
    for call in custom_api.patch_namespaced_custom_object.call_args_list:
        kwargs = call.kwargs
        assert kwargs["group"] == "keda.sh"
        assert kwargs["version"] == "v1alpha1"
        assert kwargs["namespace"] == "orchestration"
        assert kwargs["plural"] == "scaledobjects"
        annotations = kwargs["body"]["metadata"]["annotations"]
        assert annotations["autoscaling.keda.sh/paused-replicas"] == "1"
        # Wake-timestamp annotation -- wake_sleep_sensor.py's grace period
        # keys off this to avoid a stale sleep-sensor tick undoing a fresh
        # wake before the new run it's for even exists yet (found live,
        # 2026-07-20).
        assert "data-platform.internal/last-woken-at" in annotations
        called_names.add(kwargs["name"])
    assert called_names == {"dagster-webserver-scaler", "dagster-code-server-scaler"}


@patch("dagster_wake.config")
@patch("dagster_wake.client")
def test_wake_orchestration_loads_kube_config_when_not_in_cluster(mock_client, mock_config, monkeypatch):
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    mock_client.AppsV1Api.return_value.read_namespaced_deployment.return_value = _ready_deployment(1)

    dagster_wake.wake_orchestration()

    mock_config.load_kube_config.assert_called_once()
    mock_config.load_incluster_config.assert_not_called()


@patch("dagster_wake.config")
@patch("dagster_wake.client")
def test_wake_orchestration_loads_incluster_config_when_in_a_pod(mock_client, mock_config, monkeypatch):
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    mock_client.AppsV1Api.return_value.read_namespaced_deployment.return_value = _ready_deployment(1)

    dagster_wake.wake_orchestration()

    mock_config.load_incluster_config.assert_called_once()
    mock_config.load_kube_config.assert_not_called()


@patch("dagster_wake.time.sleep", return_value=None)
@patch("dagster_wake.config")
@patch("dagster_wake.client")
def test_wake_orchestration_polls_until_both_deployments_ready(mock_client, mock_config, mock_sleep):
    apps_api = MagicMock()
    mock_client.AppsV1Api.return_value = apps_api
    # webserver ready immediately, code-server ready on the 3rd poll.
    apps_api.read_namespaced_deployment.side_effect = [
        _ready_deployment(1), _ready_deployment(0),  # poll 1: webserver, code-server
        _ready_deployment(0),  # poll 2: code-server only (webserver already discarded)
        _ready_deployment(1),  # poll 3: code-server ready
    ]

    dagster_wake.wake_orchestration(poll_interval_seconds=0)

    assert apps_api.read_namespaced_deployment.call_count == 4
    assert mock_sleep.call_count == 2


@patch("dagster_wake.time.sleep", return_value=None)
@patch("dagster_wake.time.monotonic")
@patch("dagster_wake.config")
@patch("dagster_wake.client")
def test_wake_orchestration_raises_wake_error_on_timeout(mock_client, mock_config, mock_monotonic, mock_sleep):
    mock_client.AppsV1Api.return_value.read_namespaced_deployment.return_value = _ready_deployment(0)
    # deadline = start(0) + timeout(10) = 10; first loop check is 5 (under
    # deadline, keep polling), second is 15 (over deadline, raise).
    mock_monotonic.side_effect = [0, 5, 15]

    with pytest.raises(dagster_wake.WakeError, match="Timed out"):
        dagster_wake.wake_orchestration(timeout_seconds=10, poll_interval_seconds=0)
