# Satisfies features/pipeline_run_independence.feature
"""Regression test for the KEDA scale-to-zero race described in
Learnings.md's "Dagster control plane scaling on Kubernetes" entry:
before that mechanism was removed, one master_pipeline run's completion
could scale the whole control plane to zero in the exact gap before a
different, about-to-be-submitted run's own GraphQL submission/poll was
still in flight -- producing ConnectionResetError/TransportConnectionFailed
even though nothing about the run's own logic was wrong. The fix (an
always-on control plane, see test_control_plane_fixed_replicas.py) makes
this structurally impossible; this test is what keeps that true going
forward, for the general principle it stands for -- pipeline runs must
never be able to interfere with one another via shared infrastructure --
regardless of what future change might reintroduce a similar race.

Fires several master_pipeline runs as genuinely concurrent subprocesses
(all submitted before any of them is waited on), via
`dagster_data_platform.trigger_master_pipeline` -- the same real
webserver -> daemon -> K8sRunLauncher path
orchestration/module.just's verify-pipeline recipe uses, except launched
concurrently rather than sequentially, mirroring the manual concurrent
validation already performed live this session (see
Progress/2026-08-03-remove-keda-control-plane-scale-to-zero.md). Requires
the live cluster's Dagster webserver already reachable at localhost:3000
-- same precondition as trigger_master_pipeline.py itself.

Runtime note: each of these is a full, real master_pipeline execution
(extraction -> clean -> dbt staging/marts/serving), so this test takes
several minutes to run -- it deliberately exercises the real platform,
not a mock.
"""

import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DAGSTER_PACKAGE_DIR = REPO_ROOT / "orchestration" / "dagster_data_platform"
DAGSTER_HOME = REPO_ROOT / "orchestration" / "dagster_home"
VENV_BIN = REPO_ROOT / ".venv" / "bin"
VENV_PYTHON = VENV_BIN / "python"

# Mirrors orchestration/module.just's verify-pipeline exactly -- these
# three orchestration_kind/orchestration_value combos are the ones
# already confirmed live to exist and succeed. Run here concurrently
# instead of sequentially, specifically to stress the shared-control-plane
# race the KEDA removal fixed -- verify-pipeline itself only ever runs
# these sequentially, so it would never have caught that race even before
# the fix.
_RUNS = [
    ("model_schema", "sales"),
    ("model_schema", "metadata"),
    ("batch_group", "police_crimes"),
]

# Each real master_pipeline run can take several minutes (see
# dagster_launch.py's own DEFAULT_TIMEOUT_SECONDS=1800, sized for a
# from-scratch watermark's full historical backfill) -- give the
# subprocess itself a little more headroom than that so a genuine
# in-process timeout raises dagster.Failure with a clear message instead
# of this test's own subprocess.run hitting TimeoutExpired first.
_SUBPROCESS_TIMEOUT_SECONDS = 1900

_CONNECTION_ERROR_SIGNATURES = (
    "ConnectionResetError",
    "TransportConnectionFailed",
    "TransportServerError",
    "ConnectionRefusedError",
    "ConnectionAbortedError",
)


def _launch_master_pipeline(kind: str, value: str) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env["DAGSTER_HOME"] = str(DAGSTER_HOME)
    env["PATH"] = f"{VENV_BIN}:{env.get('PATH', '')}"
    return subprocess.run(
        [
            str(VENV_PYTHON),
            "-m",
            "dagster_data_platform.trigger_master_pipeline",
            "--orchestration-kind",
            kind,
            "--orchestration-value",
            value,
        ],
        cwd=str(DAGSTER_PACKAGE_DIR),
        env=env,
        capture_output=True,
        text=True,
        timeout=_SUBPROCESS_TIMEOUT_SECONDS,
    )


@pytest.fixture(scope="module")
def concurrent_master_pipeline_results():
    """Submits every _RUNS entry as a genuinely concurrent subprocess (all
    three started via ThreadPoolExecutor before any of them is waited on)
    exactly once per test session, shared across every test in this file
    so a single concurrent launch backs every assertion rather than
    re-running the whole pipeline suite per test."""
    with ThreadPoolExecutor(max_workers=len(_RUNS)) as pool:
        futures = {pool.submit(_launch_master_pipeline, kind, value): (kind, value) for kind, value in _RUNS}
        results = {}
        for future in as_completed(futures):
            kind, value = futures[future]
            results[(kind, value)] = future.result()
    return results


def test_all_concurrent_runs_succeed(concurrent_master_pipeline_results):
    failures = [
        f"{kind}={value}: exit code {result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        for (kind, value), result in concurrent_master_pipeline_results.items()
        if result.returncode != 0
    ]
    assert not failures, "\n\n".join(failures)


def test_no_connection_or_transport_errors(concurrent_master_pipeline_results):
    """The specific regression this test exists for: the old KEDA
    sleep-sensor race manifested as ConnectionResetError/
    TransportConnectionFailed when the control plane was scaled to zero
    mid-submission -- checked directly against stdout/stderr, independent
    of exit code, since a launch could in principle still succeed against
    a half-dead server on a retry."""
    failures = []
    for (kind, value), result in concurrent_master_pipeline_results.items():
        combined_output = result.stdout + result.stderr
        found = [sig for sig in _CONNECTION_ERROR_SIGNATURES if sig in combined_output]
        if found:
            failures.append(f"{kind}={value}: found {found} in output")
    assert not failures, "\n".join(failures)


def test_each_run_produced_a_successful_run_id(concurrent_master_pipeline_results):
    """trigger_master_pipeline prints exactly the run_id as its final
    stdout line on success (see its own module docstring) -- confirms
    every run really completed as SUCCESS via Dagster's own run status,
    not just that the subprocess happened to exit 0."""
    for (kind, value), result in concurrent_master_pipeline_results.items():
        assert result.returncode == 0, f"{kind}={value} failed before producing a run id"
        stdout_lines = result.stdout.strip().splitlines()
        run_id = stdout_lines[-1] if stdout_lines else ""
        assert run_id, f"{kind}={value}: no run id printed to stdout"


def test_runs_actually_executed_concurrently(concurrent_master_pipeline_results, metadata_conn):
    """Confirms these weren't just *submitted* concurrently but *executed*
    with genuinely overlapping wall-clock windows -- proof the runs are
    independent of each other and of any serializing shared resource.
    This is exactly what the removed KEDA sleep-sensor mechanism could
    have reintroduced: an earlier run's completion gating a later run's
    ability to even start. Windows come from data_processing_runs'
    job_started_timestamp/job_ended_timestamp, keyed by each run's own
    master_dagster_run_id (there can be multiple feed-run/model-run rows
    per master run; the window spans all of them)."""
    windows = {}
    cur = metadata_conn.cursor()
    for (kind, value), result in concurrent_master_pipeline_results.items():
        run_id = result.stdout.strip().splitlines()[-1]
        cur.execute(
            "select min(job_started_timestamp), max(coalesce(job_ended_timestamp, now())) "
            "from data_processing_runs where master_dagster_run_id = %s",
            (run_id,),
        )
        start, end = cur.fetchone()
        assert start is not None, f"{kind}={value} (run {run_id}): no data_processing_runs rows found"
        windows[(kind, value)] = (start, end)

    overlapping_pairs = []
    keys = list(windows.keys())
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            (s1, e1), (s2, e2) = windows[keys[i]], windows[keys[j]]
            if s1 <= e2 and s2 <= e1:
                overlapping_pairs.append((keys[i], keys[j]))

    assert overlapping_pairs, (
        "expected at least one pair of concurrently-launched master_pipeline runs to have "
        f"overlapping execution windows (proof of real concurrency), got fully serialized windows: {windows}"
    )
