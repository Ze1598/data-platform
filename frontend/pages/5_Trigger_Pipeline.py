import os

import requests
import streamlit as st
from dagster_wake import WakeError, wake_orchestration
from metadata_db import fetch_table, get_engine

st.set_page_config(page_title="Trigger Pipeline", page_icon="▶️", layout="wide")
st.title("Trigger Pipeline")
st.caption(
    "Launches master_pipeline directly through Dagster's GraphQL API -- the same single entry point "
    "every schedule/sensor/CLI trigger already goes through (Roadmap.md, 'Master pipeline "
    "orchestration'). Submits and returns immediately; it does not wait for the run to finish -- check "
    "the Recent runs table below or Dagit itself for the outcome."
)

# In-cluster DNS by default (this page runs inside the frontend Deployment,
# not on the host) -- see frontend/k8s/deployment.yaml. Falls back to
# localhost:3000 for a host-side `streamlit run` during local dev.
DAGSTER_WEBSERVER_HOST = os.environ.get("DAGSTER_WEBSERVER_HOST", "localhost")
DAGSTER_WEBSERVER_PORT = os.environ.get("DAGSTER_WEBSERVER_PORT", "3000")
GRAPHQL_URL = f"http://{DAGSTER_WEBSERVER_HOST}:{DAGSTER_WEBSERVER_PORT}/graphql"

# Mirrors dagster_graphql.DagsterGraphQLClient's own queries verbatim
# (.venv/lib/.../dagster_graphql/client/client_queries.py) rather than
# depending on the `dagster-graphql` package directly -- that package pulls
# in the full `dagster` core as a transitive dependency, a large addition
# to this otherwise-lightweight CRUD frontend for one GraphQL call.
# Verified live against this platform's real webserver before writing this
# page (both queries, both the success and the not-yet-ready-server paths).
_REPO_QUERY = """
query GetJobNames {
  repositoriesOrError {
    __typename
    ... on RepositoryConnection {
      nodes {
        name
        location { name }
        pipelines { name }
      }
    }
    ... on PythonError { message }
  }
}
"""

_SUBMIT_MUTATION = """
mutation LaunchRun($executionParams: ExecutionParams!) {
  launchPipelineExecution(executionParams: $executionParams) {
    __typename
    ... on LaunchRunSuccess { run { runId } }
    ... on LaunchPipelineRunSuccess { run { runId } }
    ... on PipelineConfigValidationInvalid { errors { message } }
    ... on PipelineNotFoundError { message }
    ... on PythonError { message }
    ... on UnauthorizedError { message }
  }
}
"""


class TriggerError(Exception):
    pass


def _resolve_master_pipeline_location(session: requests.Session) -> tuple[str, str]:
    resp = session.post(GRAPHQL_URL, json={"query": _REPO_QUERY}, timeout=10)
    resp.raise_for_status()
    result = resp.json()["data"]["repositoriesOrError"]
    if result["__typename"] != "RepositoryConnection":
        raise TriggerError(result.get("message", result["__typename"]))
    for node in result["nodes"]:
        if any(p["name"] == "master_pipeline" for p in node["pipelines"]):
            return node["location"]["name"], node["name"]
    raise TriggerError("master_pipeline not found in any repository location -- is the code server healthy?")


def trigger_master_pipeline(orchestration_kind: str, orchestration_value: str) -> str:
    session = requests.Session()
    location_name, repository_name = _resolve_master_pipeline_location(session)
    variables = {
        "executionParams": {
            "selector": {
                "repositoryLocationName": location_name,
                "repositoryName": repository_name,
                "pipelineName": "master_pipeline",
            },
            "runConfigData": {
                "ops": {
                    "run_master_pipeline": {
                        "config": {
                            "orchestration_kind": orchestration_kind,
                            "orchestration_value": orchestration_value,
                        }
                    }
                }
            },
            "mode": "default",
            "executionMetadata": {"tags": [{"key": "triggered_by", "value": "streamlit"}]},
        }
    }
    resp = session.post(GRAPHQL_URL, json={"query": _SUBMIT_MUTATION, "variables": variables}, timeout=10)
    resp.raise_for_status()
    result = resp.json()["data"]["launchPipelineExecution"]
    typename = result["__typename"]
    if typename in ("LaunchRunSuccess", "LaunchPipelineRunSuccess"):
        return result["run"]["runId"]
    raise TriggerError(f"{typename}: {result.get('message') or result.get('errors')}")


engine = get_engine()

orchestration_kind = st.radio("Orchestration kind", ["batch_group", "model_schema"], horizontal=True)

if orchestration_kind == "batch_group":
    # batch_group itself is a uuid (data_feed.batch_group) -- the
    # human-readable value, and the one PostgresMetadataResource.
    # get_batch_group_feeds() actually matches against
    # (`WHERE batch_group_friendly_name = %s`), is the separate
    # batch_group_friendly_name text column. Confirmed live: submitting
    # the uuid as orchestration_value resolves zero feeds and fails with
    # "No active feeds resolved" -- a real bug this page had, caught by
    # actually running a batch_group trigger end-to-end, not just
    # model_schema (see Learnings.md).
    values = sorted(fetch_table(engine, "data_feed", order_by="friendly_name")["batch_group_friendly_name"].dropna().unique().tolist())
    help_text = "Every data_feed row sharing this batch_group gets extracted, then its domain(s) built."
else:
    values = sorted(fetch_table(engine, "lakehouse_models", order_by="friendly_name")["model_schema"].dropna().unique().tolist())
    help_text = "Every feed feeding this domain's lakehouse_models rows gets extracted, then the domain built."

if not values:
    st.info(f"No {orchestration_kind} values found in metadata yet -- nothing to trigger.")
else:
    orchestration_value = st.selectbox("Orchestration value", values, help=help_text)

    if st.button("Trigger run", type="primary"):
        # Cooperative wake -- orchestration may currently be scaled to 0 by
        # KEDA outside a configured schedule window
        # (orchestration/k8s/keda-scaledobjects.yaml). Always wake first,
        # unconditionally: cheap/idempotent when already awake (both calls
        # inside wake_orchestration() no-op quickly), and avoids a
        # try-then-wake-then-retry round trip. Deliberately never sleeps
        # from here -- see dagster_wake.py's module docstring for why
        # (master_pipeline's own run pod needs the webserver reachable for
        # its whole duration, not just at submission); a Dagster run-status
        # sensor (wake_sleep_sensor.py) owns unpausing once it's safe.
        with st.spinner("Waking Dagster orchestration (may be scaled to zero)..."):
            try:
                wake_orchestration()
            except WakeError as e:
                st.error(f"Timed out waiting for orchestration to become ready: {e}")
                st.stop()
            except Exception as e:
                st.error(f"Could not wake orchestration (RBAC denied, or Kubernetes API unreachable): {e}")
                st.stop()

        with st.spinner(f"Submitting master_pipeline (orchestration_kind={orchestration_kind}, orchestration_value={orchestration_value})..."):
            try:
                run_id = trigger_master_pipeline(orchestration_kind, orchestration_value)
                st.success(f"Run submitted: `{run_id}`")
                st.caption(f"Dagit: http://localhost:3000/runs/{run_id}")
                st.caption(
                    "Orchestration stays awake until this run reaches a terminal status, then scales "
                    "back to zero automatically (dagster-daemon's own sensor)."
                )
            except requests.exceptions.ConnectionError:
                st.error(
                    f"Could not reach the Dagster webserver at `{GRAPHQL_URL}` even after waking "
                    "orchestration -- check `kubectl logs -n orchestration deployment/dagster-webserver`."
                )
            except (requests.exceptions.RequestException, TriggerError) as e:
                st.error(f"Trigger failed: {e}")

st.divider()
st.subheader("Recent runs")
if st.button("Refresh"):
    st.rerun()

runs = fetch_table(engine, "data_processing_runs", order_by="job_started_timestamp")
if runs.empty:
    st.info("No runs recorded yet.")
else:
    recent = runs.sort_values("job_started_timestamp", ascending=False).head(20)
    st.dataframe(
        recent[
            [
                "tracking_group",
                "tracking_group_type",
                "master_dagster_run_id",
                "job_successful",
                "job_started_timestamp",
                "job_ended_timestamp",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )
