# Satisfies features/control_plane_fixed_replicas.feature
"""Regression test for the deleted KEDA-based cooperative wake/sleep
mechanism (see Learnings.md, "Dagster control plane scaling on
Kubernetes"): dagster-webserver/dagster-daemon/dagster-code-server, in
the `orchestration` namespace, must run as plain, always-on, replicas: 1
Deployments -- matching Dagster's own documented production Kubernetes
deployment pattern -- with no autoscaler (KEDA ScaledObject, HPA, or
anything else) capable of scaling any of them to zero.

Checks the *live* Kubernetes API directly (kubernetes Python client,
default kubeconfig/context -- the same one `kubectl` already uses), not
the committed YAML under orchestration/k8s/ -- this is deliberately a
runtime check of the actual cluster state, since the whole point is to
catch drift (someone applying a ScaledObject by hand, a `kubectl scale`
down, a manifest edit that regresses replicas below 1) that a static
read of the manifests could never see.

Requires the live cluster already up and reachable via kubectl/kubeconfig
-- infra lifecycle belongs to the Architect role, not this test.
"""

import pytest
from kubernetes import client, config
from kubernetes.client.rest import ApiException

NAMESPACE = "orchestration"
CONTROL_PLANE_DEPLOYMENTS = ["dagster-webserver", "dagster-daemon", "dagster-code-server"]


@pytest.fixture(scope="module")
def k8s_apps_v1():
    config.load_kube_config()
    return client.AppsV1Api()


@pytest.fixture(scope="module")
def k8s_custom_objects():
    config.load_kube_config()
    return client.CustomObjectsApi()


@pytest.fixture(scope="module")
def k8s_autoscaling_v2():
    config.load_kube_config()
    return client.AutoscalingV2Api()


@pytest.mark.parametrize("deployment_name", CONTROL_PLANE_DEPLOYMENTS)
def test_deployment_spec_replicas_is_fixed_at_one(k8s_apps_v1, deployment_name):
    deployment = k8s_apps_v1.read_namespaced_deployment(deployment_name, NAMESPACE)
    assert deployment.spec.replicas == 1, (
        f"{deployment_name}: expected spec.replicas == 1 (always-on, fixed), got {deployment.spec.replicas}"
    )


@pytest.mark.parametrize("deployment_name", CONTROL_PLANE_DEPLOYMENTS)
def test_deployment_live_replica_count_is_never_zero(k8s_apps_v1, deployment_name):
    """The regression this specifically guards against: spec.replicas can
    say `1` while an external scale-to-zero mechanism (KEDA, an HPA, a
    manual `kubectl scale`) has actually driven the live pod count to 0.
    Checks status.replicas/status.available_replicas -- what the cluster
    is actually doing right now -- not just the declared spec."""
    deployment = k8s_apps_v1.read_namespaced_deployment(deployment_name, NAMESPACE)
    assert (deployment.status.replicas or 0) >= 1, (
        f"{deployment_name}: status.replicas is {deployment.status.replicas} -- control plane has been scaled to zero"
    )
    assert (deployment.status.available_replicas or 0) >= 1, (
        f"{deployment_name}: status.available_replicas is {deployment.status.available_replicas} -- "
        "no healthy replica currently running"
    )


def test_no_keda_scaledobject_targets_the_orchestration_namespace(k8s_custom_objects):
    """Asserts the absence of a scale-to-zero mechanism two ways: either
    the keda.sh CRD isn't installed on the cluster at all (confirmed live
    at the time this test was written -- `kubectl api-resources` has no
    `scaledobjects` type after the KEDA removal), or it is installed but
    no ScaledObject resource exists in this namespace. Either outcome is
    a pass; only a ScaledObject actually present here is a failure --
    this deliberately doesn't require the CRD's absence, so it still
    correctly passes/fails if KEDA is ever reinstalled for an unrelated
    reason without being pointed at this namespace again."""
    try:
        result = k8s_custom_objects.list_namespaced_custom_object(
            group="keda.sh", version="v1alpha1", namespace=NAMESPACE, plural="scaledobjects"
        )
    except ApiException as e:
        assert e.status == 404, f"unexpected error checking for KEDA ScaledObjects: {e.status} {e.reason}"
        return
    items = result.get("items", [])
    assert not items, f"found KEDA ScaledObject(s) targeting {NAMESPACE}: {[i['metadata']['name'] for i in items]}"


def test_no_hpa_targets_the_control_plane_deployments(k8s_autoscaling_v2):
    """Covers the other standard Kubernetes autoscale-to-zero primitive
    (HorizontalPodAutoscaler) even though this project never used one for
    the control plane -- a genuine, general assertion of "no autoscaler
    at all" for these three Deployments, not narrowly scoped to KEDA."""
    hpas = k8s_autoscaling_v2.list_namespaced_horizontal_pod_autoscaler(NAMESPACE)
    targeting_control_plane = [
        hpa.metadata.name for hpa in hpas.items if hpa.spec.scale_target_ref.name in CONTROL_PLANE_DEPLOYMENTS
    ]
    assert not targeting_control_plane, f"found HPA(s) targeting control-plane deployments: {targeting_control_plane}"
