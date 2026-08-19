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

### Run schema discovery without the frontend UI

**Scenario**: `event_timestamp_column` is null and `generate-streaming-ingestion` skips the source with "no `schema_registry` entry yet -- run 'Discover Schema' first," but driving the actual Streamlit page isn't practical (no browser available, or scripting a full validation pass). The host machine can't reach Kafka directly — `kafka.streaming.svc.cluster.local` is in-cluster-only DNS, no NodePort — so this has to run from inside a pod that already has the right Python deps; the `frontend` deployment is the one that does (`confluent_kafka`, `connectors.inference`, `metadata_db`). Confirmed live (2026-08-19, `Walkthrough_New_Streaming_Source.md`'s `inventory_events`): produce real messages onto the topic first (see the Kafka section above), then run the exact code `5_Streaming_Sources.py`'s "Discover schema now" button runs:
```bash
FRONTEND_POD=$(kubectl get pod -n frontend -l app=frontend -o jsonpath='{.items[0].metadata.name}')
cat > /tmp/discover_schema.py <<'PYEOF'
import json, sys, uuid
sys.path.insert(0, "/app/frontend")
import polars as pl
from confluent_kafka import Consumer
from connectors.inference import infer_column_definitions
from metadata_db import get_engine, write_schema_registry_version
from sqlalchemy import text

FRIENDLY_NAME = "inventory_events"  # change per source
engine = get_engine()
with engine.connect() as conn:
    row = conn.execute(text("SELECT id, topic_name FROM streaming_source WHERE friendly_name = :n"), {"n": FRIENDLY_NAME}).fetchone()
source_id, topic_name = str(row[0]), row[1]

consumer = Consumer({"bootstrap.servers": "kafka.streaming.svc.cluster.local:9092",
                      "group.id": f"schema-discovery-{uuid.uuid4()}", "auto.offset.reset": "earliest"})
consumer.subscribe([topic_name])
messages = []
for _ in range(15):
    msg = consumer.poll(timeout=2.0)
    if msg is None or msg.error():
        continue
    messages.append(json.loads(msg.value()))
consumer.close()

column_definitions = infer_column_definitions(pl.DataFrame(messages))
write_schema_registry_version(engine, controlling_object_id=source_id, controlling_object_type="streaming_source",
                               column_definitions=column_definitions, primary_key_columns=[], created_by="manual-cli")
print(f"Discovered {len(column_definitions)} column(s) from {len(messages)} message(s)")
PYEOF
kubectl cp /tmp/discover_schema.py frontend/"$FRONTEND_POD":/tmp/discover_schema.py
kubectl exec -n frontend "$FRONTEND_POD" -- python /tmp/discover_schema.py

# then set event_timestamp_column the same way "Edit existing" does:
kubectl exec -n metadata statefulset/postgres -- psql -U platform -d platform_metadata -c \
  "UPDATE streaming_source SET event_timestamp_column = '<column>' WHERE friendly_name = '<friendly_name>';"
```
Use a fresh `group.id` (a random UUID, not the source's own id) every run — a stable `group.id` commits consumed offsets, so a second run against the same source finds nothing new and the poll loop just times out silently instead of erroring.

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
