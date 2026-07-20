"""Cooperative wake-up for `orchestration` (dagster-webserver +
dagster-code-server) when KEDA has scaled it to 0 outside a configured
schedule window (orchestration/k8s/keda-scaledobjects.yaml). Used by
pages/5_Trigger_Pipeline.py before submitting a run through Dagster's
GraphQL API -- see Backlog.md, "Cooperative wake-up mechanism for Dagster
from Streamlit".

Mirrors orchestration/module.just's own `_wake-orchestration` recipe
exactly (same `autoscaling.keda.sh/paused-replicas` annotation mechanism --
NOT a plain Deployment scale, which KEDA's HPA silently reverts within
~15s once a ScaledObject targets it), just issued via the Python
`kubernetes` client instead of shelling out to kubectl.

Deliberately does NOT sleep/unpause anything -- see
orchestration/dagster_data_platform/dagster_data_platform/wake_sleep_sensor.py
for why: master_pipeline's own run pod calls back into the webserver's
GraphQL API repeatedly for its entire duration (dagster_launch.py,
up to 1800s), not just at initial submission, so unpausing right after
Streamlit's own submit call returns would break the run mid-flight once
KEDA's cooldownPeriod elapses. Sleep is owned by a Dagster run-status
sensor instead, gated on real run state.

Also stamps a wake timestamp (_WAKE_TIMESTAMP_ANNOTATION) alongside the
pause annotation -- confirmed live (2026-07-20) that without this, a
STALE sleep-sensor tick (processing an older, unrelated master_pipeline
run's terminal-status event, still working through the daemon's queue)
can race a fresh wake and remove the pause annotation before this wake's
own trigger_master_pipeline() call has actually created a new run for the
sensor to see as "in flight" -- the freshly-woken pod gets killed again
within seconds, well before it can ever become ready. The sensor honors a
grace period keyed off this timestamp to close that window. See
wake_sleep_sensor.py's own docstring for the other half of this fix.
"""

import os
import time
from datetime import datetime, timezone

from kubernetes import client, config

_NAMESPACE = "orchestration"
_KEDA_GROUP = "keda.sh"
_KEDA_VERSION = "v1alpha1"
_KEDA_PLURAL = "scaledobjects"
_SCALED_OBJECTS = ["dagster-webserver-scaler", "dagster-code-server-scaler"]
_DEPLOYMENTS = ["dagster-webserver", "dagster-code-server"]
_PAUSE_ANNOTATION = "autoscaling.keda.sh/paused-replicas"
_WAKE_TIMESTAMP_ANNOTATION = "data-platform.internal/last-woken-at"

_config_loaded = False


def _ensure_k8s_config() -> None:
    global _config_loaded
    if _config_loaded:
        return
    # KUBERNETES_SERVICE_HOST is the standard idiom for "am I running
    # inside a pod" -- true for the real in-cluster Deployment, false for
    # a host-run `streamlit run app.py` dev session or pytest, which fall
    # back to whatever kubeconfig the host already uses against the kind
    # cluster.
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        config.load_incluster_config()
    else:
        config.load_kube_config()
    _config_loaded = True


class WakeError(Exception):
    pass


def wake_orchestration(timeout_seconds: float = 120.0, poll_interval_seconds: float = 3.0) -> None:
    """Idempotent -- safe to call when orchestration is already at
    replicas:1 (the annotate and readiness-poll calls both no-op
    quickly, same as _wake-orchestration's own comment states)."""
    _ensure_k8s_config()
    custom_api = client.CustomObjectsApi()
    apps_api = client.AppsV1Api()

    woken_at = datetime.now(timezone.utc).isoformat()
    for so_name in _SCALED_OBJECTS:
        custom_api.patch_namespaced_custom_object(
            group=_KEDA_GROUP,
            version=_KEDA_VERSION,
            namespace=_NAMESPACE,
            plural=_KEDA_PLURAL,
            name=so_name,
            body={"metadata": {"annotations": {_PAUSE_ANNOTATION: "1", _WAKE_TIMESTAMP_ANNOTATION: woken_at}}},
        )

    deadline = time.monotonic() + timeout_seconds
    pending = set(_DEPLOYMENTS)
    while pending:
        for dep_name in list(pending):
            dep = apps_api.read_namespaced_deployment(dep_name, _NAMESPACE)
            if (dep.status.ready_replicas or 0) >= 1:
                pending.discard(dep_name)
        if not pending:
            return
        if time.monotonic() > deadline:
            raise WakeError(f"Timed out after {timeout_seconds}s waiting for {sorted(pending)} to become ready")
        time.sleep(poll_interval_seconds)
