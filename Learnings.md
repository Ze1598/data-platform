# Lessons Learned

Obscure, hard-won fixes and the reasoning behind them — the stuff that isn't obvious from reading the manifests/config alone. Organized by phase/component. See [Progress.md](Progress.md) for the full per-phase change log this is distilled from.

---

## Phase 1 — Metadata + CRUD (Streamlit / Postgres)

**`try` / `except` / `elif` is invalid Python syntax.**
A `try/except` block can only be followed by `else`/`finally`, never `elif`. Caught by a straightforward syntax error at runtime. Fix: nest the conditional inside the `else` block.

**`uv sync` at the workspace root silently skips member dependencies.**
The root `pyproject.toml` has no dependencies of its own (`package = false`, pure workspace container). Plain `uv sync` only syncs the root project, so `frontend/`'s dependencies (streamlit, sqlalchemy, psycopg) never got installed — and `uv run streamlit` silently fell back to a *global* streamlit install on the machine instead of erroring, masking the problem until a `ModuleNotFoundError: sqlalchemy` surfaced deeper in the app. Fix: `uv sync --all-packages` to install every workspace member. Symptom to watch for: check `ps aux` for the actual interpreter path if something "works" unexpectedly — a global install can silently paper over a broken project venv.

**SQLAlchemy `text()` mishandles a Postgres `::type` cast glued directly to a named bind parameter.**
`:connection_config::jsonb` in a raw SQL string doesn't parse as "bind param `connection_config`, then cast" — SQLAlchemy's bind-parameter regex gets confused by the second colon and passes something malformed straight to the driver, producing a `psycopg.errors.SyntaxError`. Fix: use `cast(:connection_config as jsonb)` instead — unambiguous, no adjacency issue.

**`st.dataframe` renders via canvas (glide-data-grid), not DOM text.**
Two consequences: (1) UUID columns come back from psycopg as `uuid.UUID` Python objects, which the canvas renderer serializes as unreadable byte-index dicts (`{"0":29,"1":10,...}`) instead of text — fix: stringify UUID columns before handing the DataFrame to `st.dataframe`. (2) Any browser-automation test script that scrapes `page.inner_text()` to verify table contents will silently fail to see anything in the grid — the content isn't in the DOM. Verify data-grid content by querying the database directly instead, not by scraping the rendered table.

---

## Phase 2 — kind cluster + local storage

**A `PersistentVolumeClaim` doesn't fit "many pods across many namespaces need the same directory."**
A PVC binds 1:1 to exactly one PV, and PVCs are namespace-scoped — a PVC created in namespace A can't be mounted by a pod in namespace B. For genuinely shared storage across namespaces on a single-node kind cluster, skip the PVC abstraction and mount `hostPath` directly in each pod spec that needs it. (A real single-consumer case — like Postgres's own data directory — is still a normal PVC via `volumeClaimTemplates`; this only applies to the shared-across-many-consumers case.)

**Once Postgres moves in-cluster, host-side tools need a NodePort + kind `extraPortMappings`, not just a Service.**
A plain `ClusterIP` Service is only reachable from inside the cluster. To keep `localhost:5432` working for tools still running directly on the Mac (the Streamlit app via `uv run`, a local `psql`), the Postgres Service needs `type: NodePort` with a fixed `nodePort`, and the kind cluster config needs a matching `extraPortMappings` entry (`hostPort: 5432` → `containerPort: <nodePort>`). This has to be set at `kind create cluster` time — changing it later means recreating the cluster.

**Regenerate the init-scripts ConfigMap from the SQL files directly, don't duplicate SQL into a YAML manifest.**
`kubectl create configmap postgres-init-scripts --from-file=metadata/db/init/ --dry-run=client -o yaml | kubectl apply -f -` is idempotent and keeps `01_platform_metadata.sql`/`02_polaris_db.sql` as the single source of truth. Note this only affects *fresh* Postgres initialization (an already-initialized data directory won't re-run init scripts just because the ConfigMap changed) — a new SQL file added later needs to be applied manually (`kubectl exec ... psql -c "..."`) against an already-running instance, in addition to updating the ConfigMap for future fresh installs.

---

## Phase 3 — Apache Polaris + Trino + MinIO (the hard one)

This phase had by far the most obscure, layered issues. Grouped by root cause, in the order they were actually hit.

### Polaris's `FILE` storage type — ultimately abandoned

- Setting `SUPPORTED_CATALOG_STORAGE_TYPES` to include `FILE` via env var requires **JSON array syntax**, not a comma-separated string: `POLARIS_FEATURES__SUPPORTED_CATALOG_STORAGE_TYPES_='["S3","GCS","AZURE","FILE"]'`. A comma-separated value produces a Jackson `JsonParseException` at startup.
- Even with `FILE` in the supported-types list, a **separate request-time check** (`ALLOW_INSECURE_STORAGE_TYPES`) rejects every catalog operation with "File IO implementation ... is considered insecure and must not be used."
- Polaris's **production-readiness check** for insecure storage types is a hard startup-abort (`IllegalStateException: Severe production readiness issues detected, startup aborted!`), not just a logged warning — bypassable globally via `POLARIS_READINESS_IGNORE_SEVERE_ISSUES=true`, but this bypasses *all* severe checks, not just this one.
- Even after clearing all of the above (env vars confirmed correctly populating Polaris's internal config, verified via debug logging and by cross-referencing the exact `apache-polaris-1.6.0` tagged source, not just the `main` branch), the `ALLOW_INSECURE_STORAGE_TYPES` check still silently failed to apply — root cause never fully identified. **Decision: abandon `FILE` storage entirely and pivot to MinIO (S3-compatible) instead**, which is Polaris's non-defense-gated path. Don't sink more time into `FILE` storage on Polaris ≥1.6.0.

### Quarkus / SmallRye Config env var naming, for any future Polaris config tuning

A property like `polaris.features."SOME_KEY"` (a quoted map key, because of the embedded underscores/dots) maps to the env var `POLARIS_FEATURES__SOME_KEY_` — dots and quote characters both become underscores, so a quoted map key produces a *double* underscore where the dot-then-quote or quote-then-dot collide. Verified against multiple real properties (`SUPPORTED_CATALOG_STORAGE_TYPES`, `ALLOW_INSECURE_STORAGE_TYPES`) — the pattern held consistently once the JSON-vs-plain-string value format was also correct.

### Polaris schema bootstrap is a separate, mandatory step

Setting `POLARIS_BOOTSTRAP_CREDENTIALS` on the main server container self-bootstraps the *realm and root credential record* — but does **not** create the underlying database schema tables. Querying Polaris before running the schema bootstrap fails with `relation "polaris_schema.entities" does not exist`. The schema itself has to be created via a separate one-off run of `apache/polaris-admin-tool:latest bootstrap -r <realm> -c <realm>,<clientId>,<clientSecret>` (a Kubernetes `Job`, in our case) before the main server can serve any real request, even though the server itself starts up cleanly without it.

### Polaris catalog `storageConfigInfo` — create-time only

Fields like `pathStyleAccess` and `stsUnavailable` only reliably apply when set on catalog **creation** (`POST /api/management/v1/catalogs`). A `PUT` update to an existing catalog returns `200 OK` but silently drops those fields — confirmed by re-fetching the catalog afterward and seeing `pathStyleAccess: false` regardless of what was sent. If a storage config needs to change, **delete and recreate the catalog**, don't try to update it in place. (Also: a catalog with existing namespaces/tables can't be deleted — `DROP SCHEMA` everything in it first.)

### Polaris `S3` storage type against MinIO (non-AWS S3-compatible storage)

- Requires a `roleArn` in `storageConfigInfo` even though MinIO has no real IAM/STS — any syntactically valid dummy ARN works (`arn:aws:iam::000000000000:role/minio-polaris-role`).
- Requires `stsUnavailable: true` — without it, Polaris attempts a real STS `AssumeRole` call against the dummy ARN and fails with `StsException: The security token included in the request is invalid.`
- Polaris's credential-vending path for staged table creation (Trino-side `iceberg.rest-catalog.vended-credentials-enabled=true`) is rejected outright — `Forbidden: Principal 'root' ... not authorized for op CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION` — even for the root/`service_admin` principal, on Polaris 1.6.0. This is believed to be a permission tightened by a recent CVE fix around credential vending, not a config mistake. Same pattern hit on `DROP TABLE` (`DROP_TABLE_WITH_PURGE`), left unresolved (harmless — just can't purge-drop a table via Trino right now). **Workaround**: set `vended-credentials-enabled=false` on Trino's catalog config and give Trino the object store's static credentials directly instead of relying on Polaris to vend them.
- **The actual root cause of the core blocker, and the one that cost the most time**: Polaris's own **server-side** `S3FileIO` client (used to validate/finalize table commits — a step independent of anything Trino does) was not picking up the catalog's `s3.endpoint` property at all, and defaulted to real AWS S3, failing with `301 The bucket you are attempting to access must be addressed using the specified endpoint`. This was *not* visible as a Trino-side or MinIO-side problem — confirmed by (a) reading Polaris's own log output showing it loading `S3FileIO` right before the failure, and (b) capturing live MinIO request traffic (`mc admin trace -v`) during a reproduction and confirming the failing request never reached MinIO at all. Fix: set `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_ENDPOINT_URL_S3` (the AWS SDK Java v2 env var for a custom S3 endpoint override, supported since SDK 2.28.1) **directly on the Polaris deployment itself** — a completely separate credential/endpoint path from whatever Trino's catalog properties say.
- A suspected Trino bug (`trinodb/trino#25187` — path-style-access not honored with Iceberg REST + S3) looked like the cause for a while and led to switching Trino's catalog config from the modern `fs.native-s3` to the legacy Hadoop-S3A filesystem (`fs.hadoop.enabled` + `hive.s3.*` properties). In hindsight this was probably a red herring — the real fix was Polaris-side (above) — but the legacy config is confirmed working and wasn't reverted, to avoid re-opening an expensive debugging session. Revisiting `fs.native-s3` now that the real cause is understood is a plausible but untested future cleanup.

### Useful diagnostic techniques discovered along the way (reusable beyond this project)

- **When docs and observed behavior disagree, read the actual source at the exact deployed version tag**, not `main`/`latest` docs. Confirmed via `git tag`-pinned raw GitHub URLs (e.g. `apache-polaris-1.6.0`) rather than trusting a docs page that might describe a different version.
- **When a config change "does nothing," verify it actually reached the running process** before assuming the config key is wrong — `kubectl exec ... printenv | grep ...` to confirm the env var is actually set on the live pod, rather than just re-reading the manifest.
- **Live traffic capture is the fastest way to prove "is component X actually receiving this request at all"**, cutting through speculation about which of several services in a chain is misbehaving. `mc admin trace -v` (run as a throwaway pod inside the cluster, talking to the in-cluster MinIO service) was the single most useful diagnostic step in this whole phase — it converted "maybe Trino, maybe Polaris, maybe MinIO" into "definitely Polaris, and MinIO never even saw the request."
