# Debug Reference: metadata (Postgres)

Commands for inspecting/resetting the platform's own configuration database (`source_system`, `data_feed`, `model_feed`, etc.) and, later, the Hive/Polaris-adjacent `polaris_db`. See [../platform/DebugReference.md](../platform/DebugReference.md) for general `kubectl exec` mechanics this builds on, and [../Learnings.md](../Learnings.md) for why some of this schema looks the way it does.

---

### Connect interactively
**Scenario**: poking around the schema by hand, checking row counts, one-off fixes.
```bash
kubectl exec -it -n metadata postgres-0 -- psql -U platform -d platform_metadata
```
Drops into a `psql` prompt running *inside* the already-running Postgres container (`kubectl exec` runs an additional process in an existing container — it doesn't start a new one). `\dt` lists tables, `\d <table>` describes columns, `\q` exits.

### Run a single query non-interactively
**Scenario**: scripted checks (verification scripts, CI-style smoke tests) where you want the result back in the shell, not an interactive session.
```bash
kubectl exec -n metadata postgres-0 -- psql -U platform -d platform_metadata -c "select count(*) from source_system;"
```

### Reset all metadata tables to empty
**Scenario**: before re-running an end-to-end verification script, so `INSERT`s referencing fixed test codes (`test_src`, `test_feed`) don't collide with leftovers from a previous run.
```bash
kubectl exec -n metadata postgres-0 -- psql -U platform -d platform_metadata -c \
  "truncate table model_feed_source, data_feed_run, data_model_run, model_feed, schema_registry, data_feed, source_system cascade;"
```
Order matters less than it looks because of `cascade`, but listing dependents-first is clearer to read. `cascade` is required because of the FK relationships between these tables.

### List databases
**Scenario**: confirming `polaris_db` actually got created (either by fresh-cluster init or by the manual `CREATE DATABASE` fallback used when adding it to an already-initialized cluster).
```bash
kubectl exec -n metadata postgres-0 -- psql -U platform -l
```

### Connect from the host instead of via kubectl exec
**Scenario**: using a local `psql` client or GUI tool (TablePlus, DBeaver, Postico) instead of going through the container each time — works because Postgres is exposed via a NodePort mapped to `localhost:5432` (see `metadata/k8s/service.yaml` and `platform/kind/kind-cluster.yaml`).
```bash
psql -h localhost -U platform -d platform_metadata   # password: platform (see .env)
```
