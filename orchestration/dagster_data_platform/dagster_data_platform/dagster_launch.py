"""Launch-and-wait for the master pipeline's own child-job invocations
(EXTRACTION_JOBS/MODELING_JOBS/SERVING_JOBS, see
scripts/generate_dagster_pipeline.py) -- via dagster_graphql's
DagsterGraphQLClient, the same real webserver -> daemon -> K8sRunLauncher
path trigger_schedule_run.py already uses (`dagster job launch` has no
--tags flag, and a Python-in-process job launch wouldn't go through
K8sRunLauncher at all -- see Roadmap.md "Master pipeline orchestration").

DAGSTER_WEBSERVER_HOST/PORT default to localhost:3000 -- correct when
called from the host (trigger_schedule_run.py/trigger_master_pipeline.py,
via the webserver's NodePort, orchestration/k8s/webserver-service.yaml).
Inside a launched pod (master_pipeline's own op), "localhost" resolves to
the pod itself, not the webserver, so K8sRunLauncher's run_launcher config
(dagster.yaml/dagster-incluster.yaml) overrides both env vars to the
in-cluster webserver Service's DNS name instead
(dagster-webserver.orchestration.svc.cluster.local:3000) for every pod it
launches.

Verified live against the installed dagster-graphql version:
submit_job_execution() returns a run_id string and accepts a plain
`tags=` dict; get_run_status() returns a DagsterRunStatus enum.
"""

import os
import time

from dagster import DagsterRunStatus, Failure
from dagster_graphql import DagsterGraphQLClient

_TERMINAL_STATUSES = {
    DagsterRunStatus.SUCCESS,
    DagsterRunStatus.FAILURE,
    DagsterRunStatus.CANCELED,
}

# 1800s, not 180s/900s -- a from-scratch metadata DB can mean a feed's
# watermark is empty, triggering a full historical backfill (confirmed for
# real against police_crimes' live API, see Learnings.md/module.just's
# verify-pipeline timeout comment) -- generous on purpose, sized above the
# worst real cold-start case already observed, not tuned to the common case.
DEFAULT_TIMEOUT_SECONDS = 1800.0
DEFAULT_POLL_INTERVAL_SECONDS = 5.0


def _graphql_client() -> DagsterGraphQLClient:
    host = os.environ.get("DAGSTER_WEBSERVER_HOST", "localhost")
    port = int(os.environ.get("DAGSTER_WEBSERVER_PORT", "3000"))
    return DagsterGraphQLClient(host, port_number=port)


def launch_and_wait(
    job_name: str,
    tags: dict[str, str],
    *,
    run_config: dict | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
) -> str:
    """Submits `job_name` with the given tags (and run_config, needed for
    master_pipeline itself -- its orchestration_kind/orchestration_value op
    config), polls until it reaches a terminal DagsterRunStatus, and raises
    dagster.Failure if it didn't succeed -- a Failure here propagates
    straight up through the calling op, which is what makes a child job's
    failure stop the master pipeline itself (Roadmap.md: "a failure
    records the failure in the data_processing_runs record, and
    propagates the failure to the master parent pipeline so it stops
    there"). Also used directly from the host by
    orchestration/module.just's verify-pipeline recipe to launch
    master_pipeline itself and wait on its real Dagster-level run status --
    confirmed live to be more reliable than `kubectl wait
    --for=condition=complete`, which reports the launched pod's k8s Job as
    "Complete" even when the Dagster run inside it failed (the run worker
    process's own exit code doesn't reflect step failures)."""
    client = _graphql_client()
    run_id = client.submit_job_execution(job_name, tags=tags, run_config=run_config or {})

    deadline = time.monotonic() + timeout_seconds
    status = client.get_run_status(run_id)
    while status not in _TERMINAL_STATUSES:
        if time.monotonic() > deadline:
            raise Failure(
                f"Child job {job_name!r} (run {run_id}) did not reach a terminal "
                f"status within {timeout_seconds}s -- last status was {status.value}"
            )
        time.sleep(poll_interval_seconds)
        status = client.get_run_status(run_id)

    if status != DagsterRunStatus.SUCCESS:
        raise Failure(f"Child job {job_name!r} (run {run_id}) finished with status {status.value}")

    return run_id
