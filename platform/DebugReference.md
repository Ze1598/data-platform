# Debug Reference: platform (Docker Desktop, kind, general kubectl)

Commands for the cluster-wide foundation this module owns — starting/recreating the local Kubernetes cluster, and the general Kubernetes inspection techniques used across every other module. Module-specific commands (Postgres queries, Trino queries, Polaris REST calls, etc.) live in that module's own `DebugReference.md` instead. See [../Learnings.md](../Learnings.md) for the reasoning behind *why* some of these were needed.

---

## Docker / Docker Desktop

### Restart the Docker daemon
**Scenario**: `docker` commands fail with `Cannot connect to the Docker daemon at unix:///.../docker.sock. Is the docker daemon running?` — happens after the Mac sleeps or Docker Desktop gets closed. The kind cluster survives this (its containers just pause), so this is a full recovery, not a rebuild.
```bash
open -a Docker
# then poll until it's actually up:
until docker info >/dev/null 2>&1; do sleep 10; done
```
`open -a Docker` launches the Docker Desktop app; the daemon takes 10-30s to become responsive after the app opens, hence the poll loop instead of a fixed sleep.

### Check how much memory/CPU Docker Desktop has available
**Scenario**: before sizing Helm chart resource requests (Trino/Polaris/MinIO), or when pods are getting OOMKilled/stuck Pending.
```bash
docker info --format '{{.MemTotal}}' | awk '{printf "%.1f GB total\n", $1/1024/1024/1024}'
docker info --format '{{.NCPU}}'
```
This is the *Docker Desktop VM's* allocation, not the Mac's total RAM — kind nodes run as containers inside this VM, so this number is the real ceiling for everything in the cluster combined.

---

## kind (cluster lifecycle)

### Recreate the cluster from scratch
**Scenario**: changed `platform/kind/kind-cluster.yaml` (e.g. added a new `extraPortMappings` entry) — these only take effect at cluster creation, not via any update command.
```bash
kind delete cluster --name data-platform
./scripts/bootstrap_kind.sh
```
Deletes the cluster (the kind node's Docker container) entirely, then the bootstrap script recreates it and reapplies namespaces + Postgres. Safe: `./data-lake/` is a host-mounted directory, so it survives cluster deletion; only in-cluster state (Postgres's PVC data, anything deployed manually rather than via a script) is actually lost.

### Check cluster/node status
**Scenario**: quick sanity check that the cluster is actually up before doing anything else.
```bash
kubectl get nodes
kubectl cluster-info --context kind-data-platform
```

---

## kubectl (general Kubernetes debugging)

### Watch pod status across all namespaces
**Scenario**: after applying a manifest or restarting Docker, confirm everything is `Running`/`Ready` before moving on.
```bash
kubectl get pods -A
kubectl get pods -n query-engine   # scoped to one namespace
```

### Follow a deployment's rollout and fail fast if it hangs
**Scenario**: after `kubectl apply` or `helm upgrade`, wait for the new pod to actually become ready rather than assuming `apply` succeeding means the pod is healthy.
```bash
kubectl rollout status deployment/polaris -n query-engine --timeout=120s
```
Blocks until the rollout completes or the timeout hits; non-zero exit on timeout, so it's script-safe.

### Read logs from a Deployment
**Scenario**: first thing to check when a pod is `CrashLoopBackOff`, or when a request to a service fails and you need to see the server-side error.
```bash
kubectl logs -n query-engine deployment/polaris --tail=60
```
`deployment/polaris` resolves to whichever pod currently backs that Deployment. If a Deployment has just rolled to a new pod and you need the *new* one specifically (not the old one still terminating), get the pod name explicitly:
```bash
kubectl get pods -n query-engine -l app=polaris --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}'
```

### Exec a one-off command inside a running container
**Scenario**: running a command against an already-running container without starting a new one — e.g. checking environment variables actually reached a live pod.
```bash
kubectl exec -n query-engine deployment/polaris -- printenv | grep POLARIS_FEATURES
```
The `--` separates kubectl's own flags from the command to run inside the container. Use `-it` instead of a fixed command for an interactive shell (`kubectl exec -it ... -- sh`). See the `metadata` and `query-engine` DebugReference files for concrete examples against Postgres/Trino specifically.

### Port-forward a ClusterIP service to your Mac
**Scenario**: any time you need to hit an in-cluster-only service from a script or curl command running on the host, without adding a permanent NodePort.
```bash
kubectl port-forward -n query-engine svc/polaris 8181:8181 > /tmp/polaris-pf.log 2>&1 &
```
Backgrounds the port-forward so the shell stays usable; redirect output somewhere since it logs every forwarded connection. **Kill it when done** (`pkill -f "port-forward -n query-engine svc/polaris"`) — stray forwards on a reused local port silently break the next attempt to start a new one.

### Launch a throwaway debug pod
**Scenario**: verifying a `hostPath` mount is actually visible from inside the cluster (Phase 2's storage proof), or running a client tool that isn't installed anywhere else.
```bash
kubectl run debug-pod --image=busybox:1.36 --restart=Never \
  --overrides='{"spec":{"containers":[{"name":"debug-pod","image":"busybox:1.36","command":["sh","-c","echo hello > /data-lake/raw/test.txt"],"volumeMounts":[{"name":"data-lake","mountPath":"/data-lake"}]}],"volumes":[{"name":"data-lake","hostPath":{"path":"/data-lake"}}]}}'
kubectl logs debug-pod
kubectl delete pod debug-pod --now
```
`--overrides` injects a raw pod spec fragment, which is the only way to add a custom volume/mount via `kubectl run` (its plain flags don't support volumes). Always clean these up — they don't self-delete like a completed `Job` pod does.

### Regenerate a ConfigMap from files, idempotently
**Scenario**: `metadata/db/init/` gained a new `.sql` file and the in-cluster ConfigMap needs to reflect it, without hand-copying SQL into a YAML manifest.
```bash
kubectl create configmap postgres-init-scripts \
  --from-file=metadata/db/init/ -n metadata \
  --dry-run=client -o yaml | kubectl apply -f -
```
`--dry-run=client -o yaml` generates the manifest without submitting it; piping into `kubectl apply -f -` makes the whole thing a create-or-update, safe to re-run.

### Run a one-off Job and wait for it to finish
**Scenario**: schema bootstrap, bucket creation — anything that needs to run once to completion, not stay running.
```bash
kubectl apply -f query-engine/polaris/bootstrap-job.yaml
kubectl wait --for=condition=complete job/polaris-bootstrap -n query-engine --timeout=60s
kubectl logs -n query-engine job/polaris-bootstrap
```
