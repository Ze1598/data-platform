Feature: Dagster pipeline runs are independent of each other
  As the data-platform's orchestration layer,
  I want concurrent or rapidly sequential master_pipeline runs to never
  affect one another's ability to submit or complete,
  So that no shared-infrastructure interference (a wake/sleep mechanism,
  a scale-to-zero heuristic, or anything similar) can ever cause one run's
  timing or outcome to break a different, independently-triggered run.

  Background:
    This is the regression spec for a real, reproducible race: a
    previous "cooperative wake/sleep" mechanism (KEDA scaling the Dagster
    control plane to zero between runs, guarded by a heuristic sleep
    sensor) could scale the control plane to zero in the exact gap
    between one run's completion and a different run's GraphQL
    submission still being in flight, producing
    ConnectionResetError/TransportConnectionFailed. That mechanism has
    been removed entirely (see Learnings.md, "Dagster control plane
    scaling on Kubernetes") in favor of an always-on control plane. This
    spec exists to keep that guarantee permanent: if any future change
    ever reintroduces a shared resource that one run's lifecycle can
    starve another run of, this is the scenario that should catch it.

  Scenario: Multiple master_pipeline runs triggered concurrently all complete independently
    Given the Dagster control plane (webserver, daemon, code-server) is
      up and reachable
    And three distinct, valid master_pipeline targets exist
      ("model_schema=sales", "model_schema=metadata",
      "batch_group=police_crimes")
    When all three master_pipeline runs are submitted at the same time,
      as genuinely concurrent processes rather than one after another
    Then every run reaches a terminal Dagster run status
    And every run's terminal status is SUCCESS
    And no run's stdout/stderr contains a connection or transport error
      (ConnectionResetError, TransportConnectionFailed, or similar)
    And the runs' actual execution windows (from data_processing_runs'
      job_started_timestamp/job_ended_timestamp) genuinely overlap in wall
      clock time -- proving they executed concurrently, not serialized by
      some hidden shared resource, and not merely submitted at the same
      moment while secretly queued behind one another

  Scenario: One run's completion never blocks or invalidates a still-in-flight run
    Given two or more master_pipeline runs are in flight at once
    When the earliest-finishing run reaches its terminal status
    Then no other still-in-flight run is affected by that completion
      (its submission, execution, and eventual terminal status are
      unaffected by the timing of the run that just finished)
    And this holds regardless of which run finishes first
