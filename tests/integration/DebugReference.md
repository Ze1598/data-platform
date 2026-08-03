# Debug Reference: tests/integration

Regression tests against the *live* platform (Trino/Postgres, Kubernetes, Dagster's live webserver), not unit tests with mocks — same verification philosophy as everywhere else in this project (see the other modules' `Learnings.md`/`DebugReference.md` entries). Requires the cluster up and reachable (`localhost:8080` Trino, `localhost:5432` Postgres via NodePort, `localhost:3000` Dagster webserver, default kubeconfig context pointed at the `kind` cluster).

### Run the suite
```bash
cd tests/integration
TRINO_HOST=localhost TRINO_PORT=8080 POSTGRES_HOST=localhost POSTGRES_USER=platform POSTGRES_PASSWORD=platform POSTGRES_PORT=5432 \
  ../../.venv/bin/pytest -v
```
Or via `just test integration` from the repo root (loads `POSTGRES_*`/`TRINO_*` from `.env` automatically).

`test_master_pipeline_run_independence.py` runs several full, real `master_pipeline` executions concurrently — expect the whole suite to take roughly 2 minutes, not seconds, when that file is included.

### What's covered so far
- `test_utc_consistency.py` — every `schema_registry` column with `data_type: "timestamp"` must be `with time zone` in both `clean.<feed>` and `staging.<staging_table_name>`, for every currently active feed (metadata-driven, not a hardcoded feed list). Written after finding `clean.customers` was naive while `clean.sales` was correctly tz-aware — same logical bug, two different code paths (see Learnings.md, Phase 6). Also specifically covers the *stale pre-existing table* failure mode: dbt's incremental `MERGE` doesn't retroactively fix an existing table's column types, only a fresh `CREATE TABLE AS SELECT` does — fixing the writer isn't enough by itself if an old table is still sitting there.
- `test_master_pipeline_run_independence.py` (satisfies `features/pipeline_run_independence.feature`) — fires `model_schema=sales`, `model_schema=metadata`, and `batch_group=police_crimes` `master_pipeline` runs as genuinely concurrent subprocesses (via `dagster_data_platform.trigger_master_pipeline`), asserts all succeed with no `ConnectionResetError`/`TransportConnectionFailed`-style output, and confirms their `data_processing_runs` execution windows actually overlap in wall-clock time (proof of real concurrency, not just concurrent submission). Regression test for the removed KEDA cooperative wake/sleep mechanism's race (see Learnings.md, "Dagster control plane scaling on Kubernetes") — requires the Dagster webserver reachable at `localhost:3000` and a `DAGSTER_HOME` pointed at `orchestration/dagster_home` (both handled by the test itself, no manual setup needed beyond the live cluster).
- `test_control_plane_fixed_replicas.py` (satisfies `features/control_plane_fixed_replicas.feature`) — asserts, against the live Kubernetes API (`kubernetes` Python client, default kubeconfig context), that `dagster-webserver`/`dagster-daemon`/`dagster-code-server` in the `orchestration` namespace each have `spec.replicas == 1` and a live `status.replicas`/`status.available_replicas` of at least 1, and that no KEDA `ScaledObject` or `HorizontalPodAutoscaler` targets any of them. Manual equivalent of what this test checks:
  ```bash
  kubectl get deployment dagster-webserver dagster-daemon dagster-code-server -n orchestration -o jsonpath='{range .items[*]}{.metadata.name}{" spec.replicas="}{.spec.replicas}{" status.replicas="}{.status.replicas}{"\n"}{end}'
  kubectl get scaledobjects -n orchestration   # expect either "the server doesn't have a resource type" (KEDA CRD not installed) or an empty list
  kubectl get hpa -n orchestration             # expect an empty list
  ```

### Adding a new regression test
Follow the same shape: prefer metadata-driven checks (query `data_feed`/`schema_registry`, assert against whatever's *actually* configured) over hardcoding feed names, so new feeds get covered automatically rather than requiring someone to remember to add a test for them. For a `.feature`-file-driven test, reference the `.feature` file by name in a leading comment (`# Satisfies features/<name>.feature`) — see `features/README.md` for the full convention.
