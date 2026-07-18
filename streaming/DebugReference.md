# Debug Reference: streaming (Kafka, Flink, synthetic producer)

Commands for the real-time ingestion module (Roadmap Phase 11) — Kafka (KRaft, single broker), the Flink Kubernetes Operator + a `FlinkDeployment` running a vendored Java SQL-runner driver, and a synthetic sales-event producer. See [../platform/DebugReference.md](../platform/DebugReference.md) for general `kubectl`/port-forward mechanics this builds on, and [../Learnings.md](../Learnings.md)'s "Flink + Kafka + Iceberg (streaming/ module)" section for the reasoning behind why this module's config looks the way it does (in particular: why PyFlink was abandoned, and two real Iceberg/AWS classpath/env-var gotchas).

---

## Kafka

### Produce/consume a message manually
**Scenario**: verifying the broker itself works, independent of Flink.
```bash
kubectl exec -n streaming deployment/kafka -- sh -c \
  'echo "hello" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sales-events'

kubectl exec -n streaming deployment/kafka -- sh -c \
  '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sales-events --from-beginning --max-messages 5'
```
Binary path confirmed directly against the `apache/kafka:latest` image — not on `PATH`, must use the full `/opt/kafka/bin/...` path.

### Produce a real, schema-matching test event
**Scenario**: testing the Flink sink in isolation from the producer Deployment. `event_timestamp` **must** be the SQL-standard space-separated format (`yyyy-MM-dd HH:mm:ss.SSSSSS`), not ISO-8601 `'T'`-separated — see Learnings.md, this fails the Flink-side `CAST` *silently* (no error anywhere, the row is just dropped).
```bash
kubectl exec -n streaming deployment/kafka -- sh -c '
echo "{\"event_id\":\"manual-test\",\"event_type\":\"sale\",\"branch\":\"A\",\"city\":\"Yangon\",\"product_line\":\"Health and beauty\",\"amount\":42.0,\"event_timestamp\":\"2026-07-18 12:00:00.000000\"}" | \
/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sales-events
'
```

---

## Flink

### Check the sink job's actual state
**Scenario**: `RUNNING` means the continuous Kafka→Iceberg job is healthy; anything else (`RECONCILING`, `FAILED`) needs investigation.
```bash
kubectl get flinkdeployment sales-events-sink -n streaming -o jsonpath='{.status.jobStatus.state}{"\n"}'
```
Async, like every other Kubernetes CR status in this repo (`verify-pipeline`'s job-name-diffing, `verify-schedule`'s poll) — don't trust a single `kubectl get` immediately after `kubectl apply`; poll, as `streaming/flink/module.just::start` already does.

### Read JobManager vs. TaskManager logs — different failures show up in different pods
**Scenario**: the JobManager pod (`deployment/sales-events-sink`) shows catalog/DDL-time failures (a bad `CREATE CATALOG` property, a missing class at catalog-factory time). The **TaskManager** pod (`sales-events-sink-taskmanager-*`, name changes per restart — `kubectl get pods -n streaming -l app=sales-events-sink` to find it) shows actual data-write failures (AWS credential/region issues, CAST failures) — these only surface once a task starts executing, not at job submission.
```bash
kubectl logs -n streaming deployment/sales-events-sink -c flink-main-container --tail=200
kubectl logs -n streaming sales-events-sink-taskmanager-1-1 --tail=200
```
Get the innermost cause the same way as Trino (`grep -B2 -A 40 "Caused by"`), but also check for a "real commit attempt with `dataFilesCount=0`" pattern — that's the tell for the silent-CAST-failure gotcha (no exception, but zero rows actually written despite a real commit being logged).

### Rebuild and redeploy after changing the SQL script or the driver
**Scenario**: `sql-scripts/sales_events_sink.sql` (or the vendored `sql-runner/` Java) changed — the whole thing is baked into the image at build time, no hot-reload.
```bash
cd streaming/flink
docker build --provenance=false -f Dockerfile -t data-platform-streaming-flink:latest .
kind load docker-image data-platform-streaming-flink:latest --name data-platform
kubectl delete flinkdeployment sales-events-sink -n streaming --ignore-not-found
kubectl apply -f flinkdeployment.yaml
```
(Or just `just flink::start`, which does all of this plus polls for `RUNNING`.)

### `ImagePullBackOff` on a locally-built image that was already `kind load`ed
**Scenario**: `kubectl describe pod` shows kubelet trying a real registry pull for a purely local image name (`pull access denied, repository does not exist`), even though `ctr -n k8s.io images list` (run via `docker exec data-platform-control-plane ...`) shows the image genuinely present.
```bash
docker exec data-platform-control-plane ctr -n k8s.io images list | grep <image-name>
```
If it's there via `ctr` but the pod still won't pull: check whether the image was built `--platform linux/amd64` on this project's arm64 (Apple Silicon) cluster — cross-architecture images loaded via `kind load docker-image` are not reliably visible to the CRI image service kubelet actually queries. See Learnings.md, "PyFlink is a dead end for this project's arm64 local cluster," for the full investigation — the fix used here was avoiding the cross-arch image entirely (dropping PyFlink for a native-arch Java driver), not solving the loading mechanism itself.

### Flink Kubernetes Operator install/reinstall
```bash
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/
helm upgrade --install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
    -n streaming --set webhook.create=false
```
`webhook.create=false` is required on this cluster — the default webhook needs cert-manager (`Certificate`/`Issuer` CRDs), which isn't installed anywhere else here. Without this flag, `helm install` fails outright (`no matches for kind "Certificate" in version "cert-manager.io/v1"`).

---

## Producer

### Watch events being generated
```bash
kubectl logs -n streaming deployment/sales-events-producer --tail=20 -f
```

### Confirm the pipeline is actually flowing end to end
```bash
kubectl exec -n query-engine deployment/trino-coordinator -- trino --execute \
  "SELECT count(*) FROM iceberg.streaming.sales_events"
# re-run 30s later, confirm the count increased
```

## Isolated streaming tests (`streaming/testing/`)

`just streaming-testing::test` runs the whole cycle below as one command
(also the last step of `just smoketest`, skippable via `skip_streaming=true`).
Each stage also runs standalone via `just streaming-testing::<setup|verify-raw|verify-serve>`.

### Watch a test stage's own Job while it runs
```bash
kubectl get pods -n streaming -l job-name=streaming-testing-setup -w
kubectl logs -n streaming job/streaming-testing-setup -f
```

### Manually check what a test run actually proved
```bash
# Dummy model-layer fixture (non-destructive -- see run.py's _MODEL_LAYER_FIXTURES)
kubectl exec -n query-engine deployment/trino-coordinator -- trino --execute \
  "SELECT * FROM iceberg.model.sales_dim_branch"

# The join itself resolved (zero nulls means the fixture/real dimension matched)
kubectl exec -n query-engine deployment/trino-coordinator -- trino --execute \
  "SELECT count(*) FROM iceberg.serve.sales_events WHERE city IS NULL"
```

### Re-run just the Job, without going through `just`
```bash
kubectl delete job streaming-testing-setup -n streaming --ignore-not-found
kubectl create job streaming-testing-setup -n streaming \
  --image=data-platform-streaming-testing:latest -- python run.py setup
kubectl logs -n streaming job/streaming-testing-setup -f
```
