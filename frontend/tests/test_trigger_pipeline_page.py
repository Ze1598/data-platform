"""Exercises frontend/pages/5_Trigger_Pipeline.py through Streamlit's own
headless script-running harness (AppTest) -- not a hand-copied snippet of
its backend logic, and not just "no exception raised."

Two real bugs in this page were found only by running it this way, not by
`curl`-ing its URL (200 regardless -- Streamlit serves a static shell over
plain HTTP, the actual script only runs over the browser's WebSocket
connection) or by re-typing its trigger function in isolation (proves the
GraphQL call works, never touches the page's own top-level code, which is
where both bugs lived):

1. `fetch_table(engine, "data_feed")` relied on `fetch_table`'s
   `order_by="created_at"` default, but `data_feed` has no `created_at`
   column -- crashed on page load.
2. The `batch_group` dropdown was populated from `data_feed.batch_group`
   (a uuid) instead of `data_feed.batch_group_friendly_name` (the text
   `PostgresMetadataResource.get_batch_group_feeds()` actually matches
   against) -- didn't crash, submitted successfully (`LaunchRunSuccess`),
   but the launched run always failed inside Dagster with "No active feeds
   resolved". A naive "click the button, assert no exception, accept
   st.success as proof it worked" test would have missed this entirely --
   the mutation itself genuinely succeeds; only the *run*, asynchronously,
   fails. `test_trigger_button_batch_group_resolves_real_feeds` below
   polls `data_processing_runs` for the actual submitted run_id and
   asserts it reached a real successful feed-run row, specifically to
   catch this class of bug again.

Needs the live cluster reachable (same as test_metadata_db.py) -- Postgres
via its NodePort, and the Dagster webserver at localhost:3000 (its own
NodePort, see platform/kind/kind-cluster.yaml).

Cooperative wake-up (frontend/dagster_wake.py) means the page is now
expected to succeed even when orchestration starts out scaled to zero by
KEDA -- `_click_trigger_and_get_run_id` no longer treats an `at.error`
after clicking as an acceptable "scaled to zero" outcome to skip past;
that's now a real failure. `test_trigger_button_wakes_orchestration_from_a_cold_start`
below is what actually proves cooperative wake works end to end, forcing
a genuine cold start first via `force_cold_start` rather than relying on
"outside the two real cron windows" (55 5-15 6 and 55 6-15 7 UTC), which
would make the test time-of-day-dependent and flaky.
"""

import os
import re
import time
from pathlib import Path

import psycopg
import pytest
from kubernetes import client, config
from streamlit.testing.v1 import AppTest

from metadata_db import get_engine

_PAGE_PATH = Path(__file__).resolve().parents[1] / "pages" / "5_Trigger_Pipeline.py"
_RUN_ID_RE = re.compile(r"Run submitted: `([0-9a-f-]{36})`")

_NAMESPACE = "orchestration"
_KEDA_GROUP = "keda.sh"
_KEDA_VERSION = "v1alpha1"
_KEDA_PLURAL = "scaledobjects"
_SCALED_OBJECTS = ["dagster-webserver-scaler", "dagster-code-server-scaler"]
_DEPLOYMENTS = ["dagster-webserver", "dagster-code-server"]
_PAUSE_ANNOTATION = "autoscaling.keda.sh/paused-replicas"


def _wait_for_run_success(run_id: str, timeout_seconds: float = 120.0) -> bool:
    """Polls data_processing_runs for a real successful row against this
    master_dagster_run_id -- proof the run didn't just get *accepted* by
    Dagster's GraphQL API, but actually resolved feeds and completed."""
    engine = get_engine()
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with engine.connect() as conn:
            result = conn.exec_driver_sql(
                "select count(*) from data_processing_runs where master_dagster_run_id = %s and job_successful = true",
                (run_id,),
            )
            if result.scalar() > 0:
                return True
        time.sleep(2)
    return False


def _click_trigger_and_get_run_id(at: AppTest, click_timeout: float = 150) -> str:
    # 150s, not the pre-cooperative-wake default of 60 -- every trigger
    # click now runs wake_orchestration() first (up to its own 120s
    # default readiness timeout) before the actual GraphQL submission, and
    # orchestration may legitimately be cold (KEDA scaled to 0) whenever
    # any of these tests happen to run, not just the dedicated cold-start
    # test below.
    at.button[0].click().run(timeout=click_timeout)
    assert not at.exception, f"Clicking Trigger run raised: {[str(e.value) for e in at.exception]}"
    assert not at.error, f"Trigger run failed: {[str(e.value) for e in at.error]}"
    assert at.success, "Expected a success message after clicking Trigger run"
    match = _RUN_ID_RE.search(at.success[0].value)
    assert match, f"Could not find a run_id in the success message: {at.success[0].value!r}"
    return match.group(1)


def _load_k8s_config() -> None:
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        config.load_incluster_config()
    else:
        config.load_kube_config()


@pytest.fixture
def force_cold_start():
    """Deterministically drives orchestration to a genuine scaled-to-zero
    state before the test runs, regardless of time of day -- the two real
    cron windows (55 5-15 6 and 55 6-15 7 UTC, orchestration/k8s/
    keda-scaledobjects.yaml) make "just wait outside a window" a flaky,
    time-dependent test strategy. Reuses the exact same
    `autoscaling.keda.sh/paused-replicas` annotation mechanism as
    dagster_wake.py/wake_sleep_sensor.py, just forced to "0" instead of
    removed, so KEDA's HPA doesn't fight it back up mid-setup."""
    _load_k8s_config()
    custom_api = client.CustomObjectsApi()
    apps_api = client.AppsV1Api()

    try:
        for so_name in _SCALED_OBJECTS:
            custom_api.patch_namespaced_custom_object(
                group=_KEDA_GROUP,
                version=_KEDA_VERSION,
                namespace=_NAMESPACE,
                plural=_KEDA_PLURAL,
                name=so_name,
                body={"metadata": {"annotations": {_PAUSE_ANNOTATION: "0"}}},
            )

        deadline = time.monotonic() + 60
        pending = set(_DEPLOYMENTS)
        while pending and time.monotonic() < deadline:
            for dep_name in list(pending):
                dep = apps_api.read_namespaced_deployment(dep_name, _NAMESPACE)
                if (dep.status.ready_replicas or 0) == 0:
                    pending.discard(dep_name)
            if pending:
                time.sleep(2)
        if pending:
            pytest.skip(f"Could not force a cold start -- {sorted(pending)} still reported ready replicas")
    finally:
        # Hand control back to normal pause/unpause semantics regardless
        # of whether the poll above succeeded -- the forced "0" must not
        # outlive this fixture's own setup, or it would block
        # wake_orchestration()'s own "1" from ever taking effect (last
        # merge-patch wins on the same annotation key).
        for so_name in _SCALED_OBJECTS:
            custom_api.patch_namespaced_custom_object(
                group=_KEDA_GROUP,
                version=_KEDA_VERSION,
                namespace=_NAMESPACE,
                plural=_KEDA_PLURAL,
                name=so_name,
                body={"metadata": {"annotations": {_PAUSE_ANNOTATION: None}}},
            )

    yield


def test_page_loads_without_exception():
    at = AppTest.from_file(str(_PAGE_PATH), default_timeout=30)
    at.run()
    assert not at.exception, f"Page raised on load: {[str(e.value) for e in at.exception]}"


def test_switching_orchestration_kind_does_not_raise():
    at = AppTest.from_file(str(_PAGE_PATH), default_timeout=30)
    at.run()
    assert not at.exception

    at.radio[0].set_value("model_schema").run()
    assert not at.exception, f"Switching to model_schema raised: {[str(e.value) for e in at.exception]}"

    at.radio[0].set_value("batch_group").run()
    assert not at.exception, f"Switching back to batch_group raised: {[str(e.value) for e in at.exception]}"


def test_trigger_button_batch_group_resolves_real_feeds():
    at = AppTest.from_file(str(_PAGE_PATH), default_timeout=30)
    at.run()
    assert not at.exception
    at.radio[0].set_value("batch_group").run()
    if not at.button:
        pytest.skip("No batch_group values in metadata to trigger")

    run_id = _click_trigger_and_get_run_id(at)
    assert _wait_for_run_success(run_id), (
        f"batch_group run {run_id} did not reach a successful data_processing_runs row within "
        "the timeout -- see bug #2 in this file's module docstring (batch_group value must be "
        "batch_group_friendly_name, not the raw batch_group uuid)"
    )


def test_trigger_button_model_schema_resolves_real_feeds():
    at = AppTest.from_file(str(_PAGE_PATH), default_timeout=30)
    at.run()
    assert not at.exception
    at.radio[0].set_value("model_schema").run()
    if not at.button:
        pytest.skip("No model_schema values in metadata to trigger")

    run_id = _click_trigger_and_get_run_id(at)
    assert _wait_for_run_success(run_id), f"model_schema run {run_id} did not reach a successful data_processing_runs row within the timeout"


def test_trigger_button_wakes_orchestration_from_a_cold_start(force_cold_start):
    # This is the test that actually proves cooperative wake-up
    # (Backlog.md "Cooperative wake-up mechanism for Dagster from
    # Streamlit") works end to end -- orchestration is forced to a real
    # scaled-to-zero state first, then the page is expected to wake it
    # itself and still succeed, not just fail with an explained error.
    at = AppTest.from_file(str(_PAGE_PATH), default_timeout=30)
    at.run()
    assert not at.exception
    if not at.button:
        pytest.skip("No batch_group values in metadata to trigger")

    run_id = _click_trigger_and_get_run_id(at)
    assert _wait_for_run_success(run_id, timeout_seconds=180), (
        f"Cold-start run {run_id} did not reach a successful data_processing_runs row within the timeout"
    )
