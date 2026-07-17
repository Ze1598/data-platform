# Debug Reference: orchestration (Dagster)

**Update 2026-07-17 — Dagster now runs fully in-cluster** (webserver, daemon, and a gRPC code-server as three real `Deployment`s, `orchestration/k8s/` — see Roadmap.md "Master pipeline orchestration" and `Learnings.md`'s "full in-cluster Dagster topology" entry). The **"Local dev" section below describes the OLD workflow (`dagster dev` on the host) and is no longer the standard operating mode** — kept here, not deleted, since the mechanics it explains (env vars, `dagster.yaml`, concurrency pools) are still real and occasionally useful for isolated local debugging. Each stale entry is annotated inline rather than removed. See the new **"In-cluster (current)"** section below for the actual day-to-day workflow.

Commands for running/debugging Dagster against the kind cluster's `K8sRunLauncher`. See [../platform/DebugReference.md](../platform/DebugReference.md) for general `kubectl` mechanics, and [../Learnings.md](../Learnings.md) for the reasoning behind why this module's config looks the way it does.

---

## In-cluster (current)

### Reach the webserver UI / GraphQL
**Scenario**: day-to-day — the UI, or a raw GraphQL query against the running instance. NodePort-mapped, same as Postgres/Trino, so this is unchanged whether you're checking it from the host or scripting against it.
```bash
open http://localhost:3000
curl -s localhost:3000/graphql -H 'content-type: application/json' \
  -d '{"query":"{ repositoriesOrError { ... on RepositoryConnection { nodes { jobs { name } } } } }"}'
```

### Launch a real pipeline run (not `dagster job launch -j '__ASSET_JOB'` — see below for why that's now wrong)
**Scenario**: every extraction/raw/clean asset now expects a `master_dagster_run_id` run tag and a pre-existing `data_processing_runs` row that only `master_pipeline`'s own op creates. The only correct way to launch a real run is through `master_pipeline` itself.
```bash
cd orchestration/dagster_data_platform
uv run python -m dagster_data_platform.trigger_master_pipeline \
  --orchestration-kind model_schema --orchestration-value sales
# or: --orchestration-kind batch_group --orchestration-value police_crimes
```
This blocks until the run reaches a terminal status and raises if it didn't succeed (`dagster_launch.py`'s `launch_and_wait`). For the full smoketest-equivalent check, use `just orchestration::verify-pipeline`/`verify-schedule`/`verify-sensor` directly instead of hand-rolling this.

### Rebuild + reload images (now two kinds)
**Scenario**: same as the old "Rebuild and reload the orchestration image" entry below, but there are now two image types, and a code change needs a **code-server restart**, not just a rebuild — the rebuilt image alone doesn't propagate to the already-running `dagster-code-server` pod (same `:latest` tag, no spec diff, so Kubernetes has no reason to recreate it — confirmed live, see `Learnings.md`).
```bash
just orchestration::build-image           # the shared data-platform-orchestration image
just orchestration::build-domain-images   # one data-platform-domain-<domain> image per dbt domain
kubectl rollout restart deployment/dagster-code-server -n orchestration
kubectl rollout status deployment/dagster-code-server -n orchestration
```
`just orchestration::start` already does all four steps in the right order — reach for this section only when iterating without a full `start`.

### (Re)create both instance ConfigMaps
**Scenario**: there are now two, not one — `dagster-instance` (mounted into every *launched run* pod by `K8sRunLauncher`, `load_incluster_config: false`, unchanged from before) and `dagster-instance-incluster` (mounted into the webserver/daemon themselves, `load_incluster_config: true` — the daemon is the one piece that actually calls `K8sRunLauncher.launch_run()`, so it needs in-cluster k8s API auth, not a local kubeconfig). Regenerate both from source, don't hand-edit either:
```bash
kubectl create configmap dagster-instance \
  --from-file=dagster.yaml=orchestration/dagster_home/dagster.yaml \
  -n orchestration --dry-run=client -o yaml | kubectl apply -f -

kubectl create configmap dagster-instance-incluster \
  --from-file=dagster.yaml=orchestration/dagster_home/dagster-incluster.yaml \
  --from-file=workspace.yaml=orchestration/dagster_home/workspace.yaml \
  -n orchestration --dry-run=client -o yaml | kubectl apply -f -
```

### Check daemon/webserver/code-server health directly
**Scenario**: something's not launching runs, or the webserver reports a stale asset graph — check each piece's own pod logs, not just `kubectl get pods` status.
```bash
kubectl get pods -n orchestration
kubectl logs -n orchestration deployment/dagster-daemon --tail=100
kubectl logs -n orchestration deployment/dagster-webserver --tail=100
kubectl logs -n orchestration deployment/dagster-code-server --tail=100
```
If you see repeated `"Another X daemon is still sending heartbeats"` errors, there's a **second daemon process still running somewhere** (most likely a leftover local `dagster dev` — confirmed live this session, see `Learnings.md`'s BSD-`pgrep`-alternation entry) fighting the real in-cluster one over heartbeat ownership in the shared `dagster_db`. Kill it; don't ignore the message.

### Start/stop a sensor, or check/reset its cursor, via raw GraphQL
**Scenario**: `DagsterGraphQLClient` has no built-in sensor start/stop/cursor methods — see `verify_sensor_trigger.py` for the confirmed mutation/query shapes, or query directly:
```bash
uv run python -c "
from dagster_graphql import DagsterGraphQLClient
client = DagsterGraphQLClient('localhost', port_number=3000)
selector = {'repositoryLocationName': 'dagster_data_platform', 'repositoryName': '__repository__', 'sensorName': 'financial_transactions_sensor'}
query = '''query(\$s: SensorSelector!) { sensorOrError(sensorSelector: \$s) {
  ... on Sensor { sensorState { status typeSpecificData { ... on SensorData { lastCursor } } } }
  ... on PythonError { message } } }'''
print(client._execute(query, {'s': selector}))
"
```
**Never leave a test-poisoned cursor in place** — an artificial far-future test filename permanently blinds the sensor to every real future file until manually cleared via `setSensorCursor(sensorSelector, cursor: null)` (confirmed the hard way, see `Learnings.md`).

---

## Local dev (superseded — `dagster dev` is no longer how this module runs day to day)

### Environment variables `dagster dev` needs
**Scenario**: `DAGSTER_HOME` must point at `orchestration/dagster_home/` (contains `dagster.yaml`) so the instance uses Postgres-backed storage and the K8sRunLauncher — without it, Dagster falls back to an ephemeral/local instance that a launched (in-cluster) run pod can't report status back to. `.venv/bin` must be on `$PATH` too — see "dbt adapter not found" below for why this one bites specifically.
```bash
export DAGSTER_HOME=/absolute/path/to/data-platform/orchestration/dagster_home
export POSTGRES_HOST=localhost POSTGRES_USER=platform POSTGRES_PASSWORD=platform POSTGRES_PORT=5432
export PATH="/absolute/path/to/data-platform/.venv/bin:$PATH"
```

### `Could not find adapter type trino!` even though `dbt debug` works fine directly
**Scenario**: any Dagster-launched dbt step fails with this, but running `dbt debug`/`dbt build` directly in the same shell works. Check `which dbt` — if it resolves outside `.venv/bin` (a global Python framework install, a pyenv shim, anything not this project's venv), that's the problem: `dagster_dbt`'s `DbtCliResource` invokes `dbt` via `$PATH`, and a Dagster-launched step runs in its own subprocess that inherits whatever `$PATH` looked like when the parent command started — a global `dbt` lacking the `dbt-trino` adapter shadows the correct one silently. Fix: put `.venv/bin` first on `$PATH` before running any `dagster` CLI command, not just `uv run` it (which only guarantees the right environment for the top-level command, not everything it shells out to).
```bash
which dbt   # if this isn't under .venv/bin, that's the bug
```

### Editing `dagster.yaml` while `dagster dev` is already running doesn't take effect
**Scenario**: changed `run_launcher.config.env_vars` (or anything else in `dagster.yaml`), confirmed the file is correct, but a newly launched pod still doesn't have the new values. The **daemon** (`QueuedRunCoordinatorDaemon`) — the process that actually calls `launch_run`, not the CLI command that submits a run — loaded its instance config once at its own startup and holds it in memory for as long as it keeps running. `dagster dev` needs a full restart (kill + relaunch) after any `dagster.yaml` change that affects the run launcher, storage, or concurrency config. Verify what a pod actually got, don't just trust the config file:
```bash
kubectl get job <job-name> -n orchestration -o jsonpath='{.spec.template.spec.containers[0].env}' | python3 -m json.tool
```
*(In-cluster equivalent today: `kubectl rollout restart deployment/dagster-daemon -n orchestration` after editing either `dagster.yaml`/`dagster-incluster.yaml` and re-applying the relevant ConfigMap — same "held in memory since startup" caveat applies.)*

### Run the webserver + daemon
**Scenario**: day-to-day local development — the UI (`http://localhost:3000`) plus the daemon process that actually launches queued runs (see "Why runs seemed to hang" below).
```bash
cd orchestration/dagster_data_platform
uv run dagster dev -m dagster_data_platform.definitions
```
**No longer the standard path — superseded by the in-cluster Deployment trio (see "In-cluster (current)" above).** If you ever do run this locally for isolated debugging, **first scale down the in-cluster daemon** (`kubectl scale deployment/dagster-daemon --replicas=0 -n orchestration`) and scale it back up afterward — running both simultaneously means two daemons fighting over heartbeat ownership in the same shared `dagster_db`, confirmed live to actually happen and actually break runs (see `Learnings.md`).

### Launch a run via the actual run launcher (not `dagster asset materialize`)
**Scenario**: `dagster asset materialize --select "*"` executes assets **in-process on your machine** — it never touches `K8sRunLauncher` at all, so it's useless for testing that pods actually launch in kind. Use `dagster job launch` instead, which goes through `instance.submit_run` like the UI's "Materialize" button does.
```bash
uv run dagster job launch -j '__ASSET_JOB' -m dagster_data_platform.definitions
```
**No longer a valid way to launch a real pipeline run.** `__ASSET_JOB` bypasses `master_pipeline` entirely — every extraction/raw/clean asset now expects a `master_dagster_run_id` run tag and a pre-created `data_processing_runs` row that only `master_pipeline`'s own op provides; launching `__ASSET_JOB` directly crashes immediately (`KeyError: 'master_dagster_run_id'`, confirmed live). See "Launch a real pipeline run" under "In-cluster (current)" above for the actual way.

### Why a launched run seemed to hang in `QUEUED`
**Scenario**: a run sits in `QUEUED` forever with only a `PIPELINE_ENQUEUED` event, no pod ever appears. Dagster's default `run_coordinator` is `QueuedRunCoordinator` — it does not call the run launcher directly; the **daemon** process polls the queue and does that. A one-shot CLI command with no daemon running will queue a run and then just exit, leaving it stuck. Run `dagster dev` (starts webserver + daemon together) rather than a bare CLI command if you need runs to actually launch. *(This mechanism is unchanged in-cluster — the daemon Deployment plays the identical role; this is just no longer something you'd hit locally since there's no reason to launch a bare CLI command against a local, daemon-less instance anymore.)*

**Also check this before assuming something's stuck**: a run can sit in `QUEUED` completely legitimately if another run holding the same concurrency pool slot (see below) hasn't finished yet — that's not a bug, it's the pool doing its job.
```bash
# confirm what's actually stuck:
uv run python3 -c "
from dagster import DagsterInstance
instance = DagsterInstance.get()
run = instance.get_run_by_id('<run_id>')
print(run.status)
"
```

### Concurrency pools — verify two runs of the same feed actually serialize
**Scenario**: confirming `pool=` on an asset is really blocking cross-run concurrency, not just configured and assumed to work. Launch two runs back-to-back and watch both `kubectl get pods` and each run's status.
```bash
# fire two runs nearly simultaneously:
uv run dagster job launch -j '__ASSET_JOB' -m dagster_data_platform.definitions &
uv run dagster job launch -j '__ASSET_JOB' -m dagster_data_platform.definitions &
wait

# only one pod should ever be Running at a time:
kubectl get pods -n orchestration

# one run should show STARTED, the other QUEUED, until the first finishes:
uv run python3 -c "
from dagster import DagsterInstance
instance = DagsterInstance.get()
for rid in ['<run_id_1>', '<run_id_2>']:
    print(rid, '->', instance.get_run_by_id(rid).status)
"
```
**The `dagster job launch -j '__ASSET_JOB'` calls above no longer work** (see the annotation two entries up) — substitute two `trigger_master_pipeline.py` invocations targeting the same feed's domain/batch to reproduce this check today. The verification logic itself (watch pod count, cross-check `data_processing_runs` timestamps for zero overlap) is unchanged.

Cross-check the actual timestamps afterward — `data_processing_runs`'s per-stage end timestamps should show zero overlap between the two runs, not just "eventually both succeeded" (which can also happen if they raced and got lucky). No more `layer` column to filter on since the redesign (see Learnings.md) collapsed landing/raw/clean into one row per feed per run — query `job_started_timestamp`/`<stage>_end_timestamp` directly instead (feed-run rows have `data_feed_id is not null`):
```sql
SELECT master_dagster_run_id, job_started_timestamp, raw_end_timestamp, clean_end_timestamp, job_successful
FROM data_processing_runs
WHERE data_feed_id IS NOT NULL AND master_dagster_run_id IN ('<run_id_1>', '<run_id_2>') ORDER BY job_started_timestamp;
```

---

## Kubernetes side

### Watch a launched run pod
```bash
kubectl get pods -n orchestration -w
kubectl logs -n orchestration job/dagster-run-<run_id>
```
Note: `kubectl logs` on a fast-completing Job can appear to cut off mid-line even when the run genuinely succeeded — cross-check `data_processing_runs` and the run's actual `DagsterRunStatus` (see above) rather than trusting truncated pod log output alone. **Also**: `kubectl get jobs` reporting a Job as `Complete` does **not** guarantee the Dagster run inside it succeeded — the run worker's own exit code doesn't reflect step failures (confirmed live, see `Learnings.md`). Always cross-check via `get_run_status`/`data_processing_runs`, never `kubectl`'s own Job status alone.

### Rebuild and reload the orchestration image after a code change
**Scenario**: `K8sRunLauncher` launches pods from a fixed image tag (`data-platform-orchestration:latest` in `orchestration/dagster_home/dagster.yaml`) — kind doesn't pull from a registry, so a code change needs an explicit rebuild + reload before a launched pod will see it.
```bash
cd /path/to/data-platform   # build context must be repo root, not orchestration/
docker build -f orchestration/Dockerfile -t data-platform-orchestration:latest .
kind load docker-image data-platform-orchestration:latest --name data-platform
```
**Incomplete today** — see "Rebuild + reload images (now two kinds)" under "In-cluster (current)" above: there's now also `build-domain-images` for the per-domain images, and a code-server restart is required for a structural code change to actually take effect (rebuilding+reloading the image alone doesn't restart the already-running `dagster-code-server` pod).

### (Re)create the `dagster-instance` ConfigMap
**Scenario**: `K8sRunLauncher`'s `instance_config_map` config requires this ConfigMap to exist in the `orchestration` namespace — it's mounted into every launched pod as that pod's own `DAGSTER_HOME/dagster.yaml`, so the pod's instance points at the same Postgres-backed storage the local `dagster dev` process uses. Regenerate from the source file, don't hand-edit the ConfigMap — same pattern as `postgres-init-scripts` (see `scripts/bootstrap_kind.sh`).
```bash
kubectl create configmap dagster-instance \
  --from-file=dagster.yaml=orchestration/dagster_home/dagster.yaml \
  -n orchestration --dry-run=client -o yaml | kubectl apply -f -
```
**Still accurate for this one ConfigMap** (unchanged — still what `K8sRunLauncher` mounts into launched run pods), but there's now a *second* one, `dagster-instance-incluster`, for the webserver/daemon themselves — see "In-cluster (current)" above for both.

### `ModuleNotFoundError` for a workspace member that's definitely installed
**Scenario**: `import raw_to_clean` (or any workspace member) fails even though `uv sync --all-packages` reported success and `uv pip show` finds it. See Learnings.md ("The recurring corrupted-install `.pth` issue") for the underlying cause, still not fully pinned down. Fastest workaround for a one-off command — sidesteps whatever's making Python's automatic `.pth` processing unreliable, without a full venv rebuild:
```bash
PYTHONPATH="processing/raw_to_clean:query-engine/polaris_client" python3 -c "import raw_to_clean; print('OK')"
```
If `dagster dev`/the webserver itself is affected (not just a one-off script), that needs the full fix instead:
```bash
rm -rf .venv && uv sync --all-packages
```
If that hangs on `uv cache clean` for more than a minute or two, check for overlapping fix attempts before waiting it out — `ps aux | grep uv` — and kill all of them rather than layering a third attempt on top. **Still fully relevant** — this is a local `uv`/venv issue, unrelated to whether Dagster itself runs locally or in-cluster (it also affects any local codegen script run, `pytest`, etc.).
