"""Sleep side of the cooperative wake-up mechanism (Backlog.md, "Cooperative
wake-up mechanism for Dagster from Streamlit") -- removes the KEDA
`autoscaling.keda.sh/paused-replicas` annotation frontend/dagster_wake.py
sets before submitting a run, once it's actually safe to.

Deliberately NOT owned by Streamlit itself: master_pipeline's own run pod
calls back into the webserver's GraphQL API repeatedly for its entire
duration (dagster_launch.py's launch_and_wait, submit + poll every 5s, up
to DEFAULT_TIMEOUT_SECONDS=1800s), not just at initial submission -- so
unpausing right after Streamlit's own submit call returns would let
KEDA's HPA reconcile orchestration back toward 0 (respecting
cooldownPeriod=300s) while the run is still mid-flight, breaking its own
callback calls. dagster-daemon is the correct owner instead: it's already
always-up and never a KEDA scale target (see
orchestration/k8s/keda-scaledobjects.yaml's own comment), and it already
watches run-status transitions directly against dagster_db with no
dependency on the webserver/code-server being reachable.

Gated on Dagster's own run storage (RunsFilter), not an in-process
counter -- survives daemon-pod restarts, and needs no locking for the
multi-user case (two overlapping master_pipeline runs both keep
orchestration paused until BOTH reach a terminal status).

**Real race found and fixed live (2026-07-20)**: RunsFilter alone isn't
enough. A sleep-sensor tick processing an OLDER, unrelated
master_pipeline run's terminal-status event (still working through the
daemon's queue) can fire in the exact window after
frontend/dagster_wake.py has just paused orchestration for a brand-new
trigger, but before that new trigger's own trigger_master_pipeline() call
has actually created a run for RunsFilter to see -- from this sensor's
point of view, "no other runs in flight" looks true even though a wake is
actively in progress. Confirmed live: the freshly-created dagster-webserver
pod was killed ~17s after starting, well before its own readiness probe
could ever pass. Fixed by also honoring a short grace period keyed off the
wake timestamp dagster_wake.py now stamps alongside the pause annotation
-- see _recently_woken() below.
"""

from datetime import datetime, timedelta, timezone

from kubernetes import client, config

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunsFilter,
    RunStatusSensorContext,
    run_status_sensor,
)

from dagster_data_platform.pipeline_generated import master_pipeline

_NAMESPACE = "orchestration"
_KEDA_GROUP = "keda.sh"
_KEDA_VERSION = "v1alpha1"
_KEDA_PLURAL = "scaledobjects"
_SCALED_OBJECTS = ["dagster-webserver-scaler", "dagster-code-server-scaler"]
_PAUSE_ANNOTATION = "autoscaling.keda.sh/paused-replicas"
_WAKE_TIMESTAMP_ANNOTATION = "data-platform.internal/last-woken-at"
# Generous relative to a normal wake-then-submit round trip (resolving
# master_pipeline's repository location plus one launchPipelineExecution
# mutation -- typically single-digit seconds), sized to comfortably
# outlast a slow one, not tuned to the common case. Doesn't create a
# permanent leak if a wake is never followed by a real submission: the
# next master_pipeline run's own terminal-status event (from anyone, not
# just this attempt) re-evaluates this sensor well after the grace period
# has elapsed.
_WAKE_GRACE_PERIOD_SECONDS = 60

# Mirrors dagster_launch.py's own _TERMINAL_STATUSES, inverted -- not a
# private dagster import, just the complement of that same set.
_NON_TERMINAL_STATUSES = [
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
    DagsterRunStatus.CANCELING,
]


def _recently_woken(custom_api) -> bool:
    """True if frontend/dagster_wake.py stamped a wake timestamp within
    the last _WAKE_GRACE_PERIOD_SECONDS. A missing or unparseable
    timestamp means there's nothing to protect against, not an error --
    treated as "not recently woken", not swallowed-and-retried."""
    try:
        scaled_object = custom_api.get_namespaced_custom_object(
            group=_KEDA_GROUP,
            version=_KEDA_VERSION,
            namespace=_NAMESPACE,
            plural=_KEDA_PLURAL,
            name=_SCALED_OBJECTS[0],
        )
    except Exception:
        return False
    raw = (scaled_object.get("metadata", {}).get("annotations") or {}).get(_WAKE_TIMESTAMP_ANNOTATION)
    if not raw:
        return False
    try:
        woken_at = datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        return False
    return datetime.now(timezone.utc) - woken_at < timedelta(seconds=_WAKE_GRACE_PERIOD_SECONDS)


def _sleep_if_no_other_runs_in_flight(context: RunStatusSensorContext) -> None:
    other_runs = context.instance.get_runs(
        filters=RunsFilter(job_name="master_pipeline", statuses=_NON_TERMINAL_STATUSES)
    )
    if other_runs:
        context.log.info(
            f"{len(other_runs)} other master_pipeline run(s) still in flight -- leaving orchestration paused."
        )
        return

    config.load_incluster_config()
    custom_api = client.CustomObjectsApi()

    if _recently_woken(custom_api):
        context.log.info(
            f"orchestration was woken within the last {_WAKE_GRACE_PERIOD_SECONDS}s -- leaving it "
            "paused a bit longer in case a fresh trigger's own run hasn't been created yet."
        )
        return

    for so_name in _SCALED_OBJECTS:
        try:
            custom_api.patch_namespaced_custom_object(
                group=_KEDA_GROUP,
                version=_KEDA_VERSION,
                namespace=_NAMESPACE,
                plural=_KEDA_PLURAL,
                name=so_name,
                body={"metadata": {"annotations": {_PAUSE_ANNOTATION: None}}},
            )
        except Exception as e:
            # Best-effort, matching _sleep-orchestration's own `|| true`
            # convention -- a failed unpause here just means orchestration
            # stays awake a bit longer, not a broken run.
            context.log.warning(f"Failed to remove KEDA pause annotation on {so_name}: {e}")


# default_status=RUNNING, unlike pipeline_generated.py's generated
# schedules/sensors (those default to STOPPED, a per-trigger opt-in) --
# this is infrastructure housekeeping that must be active from first
# deploy, or a paused ScaledObject would never get released at all.
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[master_pipeline],
    default_status=DefaultSensorStatus.RUNNING,
    name="master_pipeline_sleep_on_success",
)
def master_pipeline_sleep_on_success(context: RunStatusSensorContext):
    _sleep_if_no_other_runs_in_flight(context)


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    monitored_jobs=[master_pipeline],
    default_status=DefaultSensorStatus.RUNNING,
    name="master_pipeline_sleep_on_failure",
)
def master_pipeline_sleep_on_failure(context: RunStatusSensorContext):
    _sleep_if_no_other_runs_in_flight(context)


@run_status_sensor(
    run_status=DagsterRunStatus.CANCELED,
    monitored_jobs=[master_pipeline],
    default_status=DefaultSensorStatus.RUNNING,
    name="master_pipeline_sleep_on_canceled",
)
def master_pipeline_sleep_on_canceled(context: RunStatusSensorContext):
    _sleep_if_no_other_runs_in_flight(context)


ALL_WAKE_SLEEP_SENSORS = [
    master_pipeline_sleep_on_success,
    master_pipeline_sleep_on_failure,
    master_pipeline_sleep_on_canceled,
]
