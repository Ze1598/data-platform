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
NodePort, see platform/kind/kind-cluster.yaml). The two "resolves real
feeds" tests below are skipped (not failed) if orchestration is currently
scaled to zero (KEDA) -- they need a real run to actually execute, not
just to be accepted; `test_trigger_button_does_not_raise` alone still
covers the scaled-to-zero explained-error path.
"""

import re
import time
from pathlib import Path

import psycopg
import pytest
from streamlit.testing.v1 import AppTest

from metadata_db import get_engine

_PAGE_PATH = Path(__file__).resolve().parents[1] / "pages" / "5_Trigger_Pipeline.py"
_RUN_ID_RE = re.compile(r"Run submitted: `([0-9a-f-]{36})`")


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


def _click_trigger_and_get_run_id(at: AppTest) -> str | None:
    at.button[0].click().run(timeout=60)
    assert not at.exception, f"Clicking Trigger run raised: {[str(e.value) for e in at.exception]}"
    if at.error:
        pytest.skip(f"Orchestration unreachable (likely scaled to zero): {at.error[0].value}")
    assert at.success, "Expected a success message after clicking Trigger run"
    match = _RUN_ID_RE.search(at.success[0].value)
    assert match, f"Could not find a run_id in the success message: {at.success[0].value!r}"
    return match.group(1)


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


def test_trigger_button_does_not_raise():
    at = AppTest.from_file(str(_PAGE_PATH), default_timeout=30)
    at.run()
    assert not at.exception

    if not at.button:
        # No batch_group values in metadata at all -- the "nothing to
        # trigger" info branch rendered instead, nothing to click.
        return

    at.button[0].click().run(timeout=60)
    assert not at.exception, f"Clicking Trigger run raised: {[str(e.value) for e in at.exception]}"
    # Either a real run got submitted (st.success) or the page's own
    # explained connection-error path fired (st.error) -- both are
    # `AppTest`-visible as markdown elements, neither is an unhandled
    # exception, which is the actual thing this test guards against.
    # NOT proof the submitted run itself succeeds -- see the two tests
    # below for that; a `LaunchRunSuccess` mutation result and a
    # successful pipeline run are two different things (bug #2 above).
    assert at.success or at.error, "Expected either a success or an explained error message after clicking Trigger run"


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
