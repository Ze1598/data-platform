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
**Scenario**: every other Polaris REST call needs a bearer token first — this is always the first step.
```bash
TOKEN=$(curl -s http://localhost:8181/api/catalog/v1/oauth/tokens \
  --user root:s3cr3t \
  -d 'grant_type=client_credentials' \
  -d 'scope=PRINCIPAL_ROLE:ALL' | python3 -c "import json,sys;print(json.load(sys.stdin)['access_token'])")
```
`--user root:s3cr3t` sends HTTP Basic auth with the bootstrap client ID/secret; the response is a JSON blob, piped through Python to pull out just the `access_token` field into a shell variable.

### Create a catalog
**Scenario**: registering a new Iceberg catalog against a storage backend. Must be done at creation time with the full config — see Learnings.md for why `PUT` updates aren't reliable for storage config fields.
```bash
curl -s -X POST http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "name": "data_platform", "type": "INTERNAL",
      "properties": {"default-base-location": "s3://lakehouse", "s3.endpoint": "http://minio.query-engine.svc.cluster.local:9000", "s3.path-style-access": "true", "s3.access-key-id": "admin", "s3.secret-access-key": "password", "s3.region": "us-east-1"},
      "storageConfigInfo": {"roleArn": "arn:aws:iam::000000000000:role/minio-polaris-role", "storageType": "S3", "pathStyleAccess": true, "stsUnavailable": true, "allowedLocations": ["s3://lakehouse/*"]}
    }
  }' -w "\nHTTP:%{http_code}\n"
```
`-w "\nHTTP:%{http_code}\n"` prints the HTTP status code after the response body — useful since Polaris returns `200`/`201` on success and various 4xx codes with a JSON `error` body on failure, and it's easy to miss which happened just from the body alone.

### Delete a catalog
**Scenario**: recreating a catalog with different storage config (since updates don't reliably apply). Must drop all schemas/tables in it first via Trino, or this fails with "not empty".
```bash
curl -s -X DELETE http://localhost:8181/api/management/v1/catalogs/data_platform \
  -H "Authorization: Bearer $TOKEN" -w "\nHTTP:%{http_code}\n"
```

### The reusable script version
See [polaris/register-catalog.sh](polaris/register-catalog.sh) — wraps the token-fetch + create-catalog sequence above into one idempotent-ish script (fails loudly with 409 if the catalog already exists, rather than silently doing nothing).

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
