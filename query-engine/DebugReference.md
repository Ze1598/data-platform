# Debug Reference: query-engine (Trino, Apache Polaris, MinIO)

Commands for the Iceberg lakehouse query layer — the Helm-deployed Trino, the Polaris REST catalog, and MinIO (S3-compatible object storage). See [../platform/DebugReference.md](../platform/DebugReference.md) for general `kubectl`/port-forward mechanics this builds on, and [../Learnings.md](../Learnings.md) for the (extensive) reasoning behind why this module's config looks the way it does.

---

## Helm

### Add a chart repo and see its actual default values
**Scenario**: before writing a `values.yaml` override, see the *real* current defaults rather than trusting a blog post or old docs snapshot — chart defaults drift between versions.
```bash
helm repo add trino https://trinodb.github.io/charts/
helm repo update
helm show values trino/trino > /tmp/trino-values.yaml
grep -n "^catalogs" -A 15 /tmp/trino-values.yaml
```

### Install / upgrade a release with local overrides
**Scenario**: standard deploy-or-update cycle for Trino.
```bash
helm install trino trino/trino -n query-engine -f query-engine/trino/values.yaml
# later, after editing values.yaml:
helm upgrade trino trino/trino -n query-engine -f query-engine/trino/values.yaml
```

### Quick one-off override without editing the values file
**Scenario**: testing a config change live before committing it to `values.yaml` — much faster iteration than edit-save-upgrade for a single property.
```bash
helm upgrade trino trino/trino -n query-engine -f query-engine/trino/values.yaml \
  --set catalogs.iceberg="connector.name=iceberg
iceberg.catalog.type=rest
..."
```
Once confirmed working, move the change into the actual `values.yaml` file — `--set` overrides are easy to forget about and don't survive being reapplied from the file alone.

---

## Apache Polaris (REST API)

All of these assume a port-forward to Polaris is already running (`kubectl port-forward -n query-engine svc/polaris 8181:8181 &`).

### Get an OAuth access token
**Scenario**: the Polaris CLI (used everywhere below) handles auth itself — this manual token fetch is only needed if you're hitting the raw REST API directly (e.g. checking the Iceberg REST `/v1/config` endpoint for something the CLI doesn't expose).
```bash
TOKEN=$(curl -s http://localhost:8181/api/catalog/v1/oauth/tokens \
  --user root:s3cr3t \
  -d 'grant_type=client_credentials' \
  -d 'scope=PRINCIPAL_ROLE:ALL' | python3 -c "import json,sys;print(json.load(sys.stdin)['access_token'])")
```
`--user root:s3cr3t` sends HTTP Basic auth with the bootstrap client ID/secret; the response is a JSON blob, piped through Python to pull out just the `access_token` field into a shell variable.

### Create, inspect, and update a catalog — use the CLI, not hand-rolled REST
**Scenario**: any catalog admin operation beyond a one-off diagnostic query. Use the official [Polaris CLI](https://polaris.apache.org/releases/1.5.0/command-line-interface/) via `uvx` (no permanent install needed) — it handles auth itself (no manual token fetch) and constructs requests correctly (`currentEntityVersion`, property merge semantics) in a way hand-rolled `curl` has twice been proven not to (see Learnings.md: "DROP_WITH_PURGE_ENABLED...", "The RBAC privilege gap..." — both involved a `curl -X PUT`/`POST` that silently no-opped with no error). `--base-url` doesn't work reliably against this server (404s on token fetch) — use `--host`/`--port`.
```bash
# create — storageConfigInfo (role-arn/path-style-access/no-sts) can only
# be set here; there is no supported update path for it, CLI or otherwise.
uvx --from apache-polaris polaris --host localhost --port 8181 \
  --client-id root --client-secret s3cr3t \
  catalogs create data_platform \
  --storage-type s3 --default-base-location s3://lakehouse \
  --allowed-location "s3://lakehouse/*" \
  --role-arn arn:aws:iam::000000000000:role/minio-polaris-role \
  --path-style-access --no-sts \
  --property polaris.config.drop-with-purge.enabled=true

# inspect (properties + storageConfigInfo + entityVersion):
uvx --from apache-polaris polaris --host localhost --port 8181 \
  --client-id root --client-secret s3cr3t catalogs get data_platform

# update a property on an existing catalog (properties only — see above):
uvx --from apache-polaris polaris --host localhost --port 8181 \
  --client-id root --client-secret s3cr3t \
  catalogs update --set-property polaris.config.drop-with-purge.enabled=true data_platform
```
Verify a property update actually persisted via `catalogs get` afterward (`entityVersion` should have incremented) — don't trust the command's exit code alone.

### Delete a catalog
**Scenario**: recreating a catalog from scratch, since storage config has no update path. Must drop all schemas/tables in it first via Trino, or this fails with "not empty".
```bash
uvx --from apache-polaris polaris --host localhost --port 8181 \
  --client-id root --client-secret s3cr3t catalogs delete data_platform
```

### The reusable script version
See [polaris/register-catalog.sh](polaris/register-catalog.sh) — a genuinely idempotent script: checks whether the catalog exists and branches to create-or-update, and grants `catalog_admin` the `TABLE_WRITE_DATA` privilege (see below) as part of the same run. Safe to re-run on a fresh realm or an already-provisioned one. Delegates to the `polaris_client` Python module below rather than driving the CLI directly.

### Grant (or revoke) a privilege on a catalog role
**Scenario**: an operation fails with `Forbidden: Principal '...' ... not authorized for op X` even for `root`/`service_admin`. This is Polaris's RBAC authorizer, not a hardcoded lockdown — check what privilege `X` actually needs (`RbacOperationSemantics.java` in the Polaris source, pinned to the deployed version tag) and grant the *specific* privilege that operation requires, not the nearest broad one that happens to include it. `TABLE_WRITE_DATA` (needed for real table purges and staged-write delegation) is only satisfied by `CATALOG_MANAGE_CONTENT` or itself — `CATALOG_MANAGE_METADATA` does *not* imply it, so a catalog role with only metadata-management privileges fails on those two operations. See Learnings.md ("The RBAC privilege gap...") for the full trace and why `TABLE_WRITE_DATA` (not the broader `CATALOG_MANAGE_CONTENT`) is the correct grant here.
```bash
# see what a role currently has:
uvx --from apache-polaris polaris --host localhost --port 8181 \
  --client-id root --client-secret s3cr3t \
  privileges list --catalog data_platform --catalog-role catalog_admin

# grant the specific privilege actually needed:
uvx --from apache-polaris polaris --host localhost --port 8181 \
  --client-id root --client-secret s3cr3t \
  privileges catalog grant --catalog data_platform --catalog-role catalog_admin TABLE_WRITE_DATA

# revoke follows the same shape:
uvx --from apache-polaris polaris --host localhost --port 8181 \
  --client-id root --client-secret s3cr3t \
  privileges catalog revoke --catalog data_platform --catalog-role catalog_admin TABLE_WRITE_DATA
```

### Interacting with Polaris from Python — use `polaris_client`, not a CLI subprocess
**Scenario**: anything programmatic (a Dagster op, a maintenance job, a test) that needs to talk to Polaris. `query-engine/polaris_client/` is a uv workspace member wrapping `apache_polaris.sdk.management.PolarisDefaultApi` — the same generated client the `polaris` CLI itself is built on, not a subprocess wrapper around it. See Learnings.md ("`apache-polaris` ships a real Python SDK...") for why this exists and how it was verified.
```python
from polaris_client import PolarisClient

client = PolarisClient(host="localhost", port=8181, client_id="root", client_secret="s3cr3t")
# or: client = PolarisClient.from_env()  # POLARIS_HOST/PORT/CLIENT_ID/CLIENT_SECRET

client.catalog_exists("data_platform")
client.get_catalog("data_platform")
client.set_catalog_property("data_platform", "polaris.config.drop-with-purge.enabled", "true")
client.grant_catalog_privilege("data_platform", "catalog_admin", "TABLE_WRITE_DATA")
```
For scripts running outside the cluster, `polaris_client.port_forward.kubectl_port_forward` is a context manager wrapping `kubectl port-forward` (see `polaris_client/bootstrap.py` for the full pattern). Not needed by anything running as a pod inside the cluster — those reach Polaris directly via `polaris.query-engine.svc.cluster.local:8181`.

---

## MinIO

### Create a bucket (no local `mc` install needed)
**Scenario**: MinIO doesn't auto-create buckets on startup — this has to happen once before Polaris/Trino can write anything.
```bash
kubectl run mc-bucket --image=minio/mc:latest --restart=Never -n query-engine --command -- sh -c \
  "mc alias set local http://minio:9000 admin password && mc mb --ignore-existing local/lakehouse"
kubectl logs -n query-engine mc-bucket
kubectl delete pod mc-bucket -n query-engine --now
```
Runs the official `mc` (MinIO Client) image as a throwaway in-cluster pod rather than installing `mc` locally. `--ignore-existing` makes it safe to re-run. The committed, reusable version of this is [minio/create-bucket-job.yaml](minio/create-bucket-job.yaml) (a proper `Job`, not an ad hoc `kubectl run`).

### Capture live request traffic (the single most useful MinIO debugging technique found)
**Scenario**: something upstream (Trino, or in our case Polaris) is failing with an S3-flavored error, and you need to know whether the request is even reaching MinIO, and if so, exactly what it looked like.
```bash
kubectl run mc-trace --image=minio/mc:latest --restart=Never -n query-engine --command -- sh -c \
  "mc alias set local http://minio:9000 admin password && mc admin trace -v local" &
kubectl logs -n query-engine mc-trace -f > /tmp/mc-trace-live.log 2>&1 &
# ... reproduce the failing operation from the other system now ...
grep -n "301\|PermanentRedirect" /tmp/mc-trace-live.log   # or whatever error you're chasing
```
`mc admin trace -v` streams every incoming request/response (full headers, status codes) live. This is what proved a "301 must be addressed using the specified endpoint" error was coming from Polaris's internal client and never actually reaching MinIO — the trace showed zero matching requests during a reproduction that definitely triggered the error elsewhere. Remember to clean up the trace pod afterward (`kubectl delete pod mc-trace -n query-engine --now`).

---

## Trino

### Run a single query non-interactively
**Scenario**: the day-to-day way of testing anything Iceberg/Polaris/MinIO-related — schema changes, MERGE behavior, verifying a fix actually worked.
```bash
kubectl exec -n query-engine deployment/trino-coordinator -- trino --execute "SHOW SCHEMAS FROM iceberg"
```

### Get the full stack trace behind a failure
**Scenario**: the default error message is usually a one-liner ("Failed to create transaction") that hides the actual root cause several layers down. Always reach for `--debug` before guessing at a fix.
```bash
kubectl exec -n query-engine deployment/trino-coordinator -- trino --debug --execute "CREATE TABLE iceberg.clean.customers (...) WITH (format='PARQUET')" 2>&1 | grep -B2 -A 15 "Caused by"
```
`grep -B2 -A 15 "Caused by"` finds the innermost exception cause in a long Java stack trace (there are often several nested "Caused by" sections — the last one is usually the real root cause).

### The actual `MERGE` proof used in Phase 3
**Scenario**: verifying the exact upsert mechanism the whole staging-layer design depends on: existing rows update, new rows insert, untouched rows stay untouched.
```sql
MERGE INTO iceberg.staging.customers t
USING iceberg.clean.customers s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET name = s.name, email = s.email, updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (customer_id, name, email, updated_at) VALUES (s.customer_id, s.name, s.email, s.updated_at);
```
Run via `kubectl exec ... trino --execute "..."` like the other queries above. This is the manual version of what a dbt incremental model's `merge` strategy will generate automatically from Phase 4 onward.
