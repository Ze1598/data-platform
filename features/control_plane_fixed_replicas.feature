Feature: The Dagster control plane runs at a fixed replica count, never scale-to-zero
  As the data-platform's orchestration layer,
  I want dagster-webserver, dagster-daemon, and dagster-code-server to run
  as small, always-on singleton Deployments (replicas: 1, fixed),
  So that the control plane matches Dagster's own documented production
  Kubernetes deployment pattern, and no scale-to-zero mechanism can ever
  reintroduce the shared-infrastructure race described in
  pipeline_run_independence.feature.

  Background:
    A prior "cooperative wake/sleep" mechanism used KEDA to scale
    dagster-webserver/dagster-daemon/dagster-code-server to zero outside
    configured schedule windows. It was found to have a structural race
    (see Learnings.md, "Dagster control plane scaling on Kubernetes") and
    was checked against Dagster's own documented practice: none of
    Dagster's published Kubernetes deployment patterns (Helm chart,
    multi-code-location scaling case studies) scale these three
    Deployments to zero -- they are treated as small, always-on
    singletons, with real elasticity happening one layer down (ephemeral
    per-run Job pods) instead. The fix reverted to that pattern and
    deleted the wake/sleep mechanism (the annotation, the sensor, the
    Streamlit-side wake call, the KEDA ScaledObjects, and the
    cross-namespace RBAC grant it needed) entirely, not just disabled it.

  Scenario Outline: Each control-plane Deployment is configured for a fixed, non-zero replica count
    Given the orchestration namespace's Kubernetes Deployments
    When I inspect the live spec of "<deployment>"
    Then its spec.replicas is exactly 1
    And it is never configured to scale below 1 by any mechanism

    Examples:
      | deployment            |
      | dagster-webserver     |
      | dagster-daemon        |
      | dagster-code-server   |

  Scenario Outline: Each control-plane Deployment's live pod count is never zero
    Given the orchestration namespace's Kubernetes Deployments are running
    When I inspect the live status of "<deployment>" via the Kubernetes API
    Then status.replicas is at least 1
    And status.available_replicas is at least 1

    Examples:
      | deployment            |
      | dagster-webserver     |
      | dagster-daemon        |
      | dagster-code-server   |

  Scenario: No KEDA ScaledObject targets the orchestration namespace
    Given the orchestration namespace exists on the live cluster
    When I query the Kubernetes API for KEDA ScaledObject custom resources
      in that namespace
    Then either the keda.sh CRD is not installed on the cluster at all,
      or no ScaledObject resource exists in the orchestration namespace

  Scenario: No HorizontalPodAutoscaler targets any control-plane Deployment
    Given the orchestration namespace exists on the live cluster
    When I query the Kubernetes API for HorizontalPodAutoscaler resources
      in that namespace
    Then none of them target dagster-webserver, dagster-daemon, or
      dagster-code-server as their scale target
