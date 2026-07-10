# Debug Reference: orchestration (Dagster)

Commands for running Dagster locally against the kind cluster's `K8sRunLauncher`. See [../platform/DebugReference.md](../platform/DebugReference.md) for general `kubectl` mechanics, and [../Learnings.md](../Learnings.md) for the reasoning behind why this module's config looks the way it does.

---

## Local dev

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

### Run the webserver + daemon
**Scenario**: day-to-day local development — the UI (`http://localhost:3000`) plus the daemon process that actually launches queued runs (see "Why runs seemed to hang" below).
```bash
cd orchestration/dagster_data_platform
uv run dagster dev -m dagster_data_platform.definitions
```

### Launch a run via the actual run launcher (not `dagster asset materialize`)
**Scenario**: `dagster asset materialize --select "*"` executes assets **in-process on your machine** — it never touches `K8sRunLauncher` at all, so it's useless for testing that pods actually launch in kind. Use `dagster job launch` instead, which goes through `instance.submit_run` like the UI's "Materialize" button does.
```bash
uv run dagster job launch -j '__ASSET_JOB' -m dagster_data_platform.definitions
```

### Why a launched run seemed to hang in `QUEUED`
**Scenario**: a run sits in `QUEUED` forever with only a `PIPELINE_ENQUEUED` event, no pod ever appears. Dagster's default `run_coordinator` is `QueuedRunCoordinator` — it does not call the run launcher directly; the **daemon** process polls the queue and does that. A one-shot CLI command with no daemon running will queue a run and then just exit, leaving it stuck. Run `dagster dev` (starts webserver + daemon together) rather than a bare CLI command if you need runs to actually launch.

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
Cross-check the actual timestamps afterward — `data_feed_run`'s per-stage end timestamps should show zero overlap between the two runs, not just "eventually both succeeded" (which can also happen if they raced and got lucky). No more `layer` column to filter on since the redesign (see Learnings.md) collapsed landing/raw/clean into one row per feed per run — query `job_started_timestamp`/`<stage>_end_timestamp` directly instead:
```sql
SELECT dagster_run_id, job_started_timestamp, landing_end_timestamp, raw_end_timestamp, clean_end_timestamp, job_successful
FROM data_feed_run
WHERE dagster_run_id IN ('<run_id_1>', '<run_id_2>') ORDER BY job_started_timestamp;
```

---

## Kubernetes side

### Watch a launched run pod
```bash
kubectl get pods -n orchestration -w
kubectl logs -n orchestration job/dagster-run-<run_id>
```
Note: `kubectl logs` on a fast-completing Job can appear to cut off mid-line even when the run genuinely succeeded — cross-check `data_feed_run`/`data_model_run` and the run's actual `DagsterRunStatus` (see above) rather than trusting truncated pod log output alone.

### Rebuild and reload the orchestration image after a code change
**Scenario**: `K8sRunLauncher` launches pods from a fixed image tag (`data-platform-orchestration:latest` in `orchestration/dagster_home/dagster.yaml`) — kind doesn't pull from a registry, so a code change needs an explicit rebuild + reload before a launched pod will see it.
```bash
cd /path/to/data-platform   # build context must be repo root, not orchestration/
docker build -f orchestration/Dockerfile -t data-platform-orchestration:latest .
kind load docker-image data-platform-orchestration:latest --name data-platform
```

### (Re)create the `dagster-instance` ConfigMap
**Scenario**: `K8sRunLauncher`'s `instance_config_map` config requires this ConfigMap to exist in the `orchestration` namespace — it's mounted into every launched pod as that pod's own `DAGSTER_HOME/dagster.yaml`, so the pod's instance points at the same Postgres-backed storage the local `dagster dev` process uses. Regenerate from the source file, don't hand-edit the ConfigMap — same pattern as `postgres-init-scripts` (see `scripts/bootstrap_kind.sh`).
```bash
kubectl create configmap dagster-instance \
  --from-file=dagster.yaml=orchestration/dagster_home/dagster.yaml \
  -n orchestration --dry-run=client -o yaml | kubectl apply -f -
```

### `ModuleNotFoundError` for a workspace member that's definitely installed
**Scenario**: `import raw_to_clean` (or any workspace member) fails even though `uv sync --all-packages` reported success and `uv pip show` finds it. See Learnings.md ("The recurring corrupted-install `.pth` issue") for the underlying cause, still not fully pinned down. Fastest workaround for a one-off command — sidesteps whatever's making Python's automatic `.pth` processing unreliable, without a full venv rebuild:
```bash
PYTHONPATH="processing/raw_to_clean:query-engine/polaris_client" python3 -c "import raw_to_clean; print('OK')"
```
If `dagster dev`/the webserver itself is affected (not just a one-off script), that needs the full fix instead:
```bash
rm -rf .venv && uv sync --all-packages
```
If that hangs on `uv cache clean` for more than a minute or two, check for overlapping fix attempts before waiting it out — `ps aux | grep uv` — and kill all of them rather than layering a third attempt on top.
