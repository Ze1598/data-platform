# Progress Tracker

Tracks implementation of [Roadmap.md](Roadmap.md), phase by phase. Update checkboxes and the "Notes / Deviations" line as work happens — this file reflects actual state, the Roadmap reflects intended design. If implementation diverges from the Roadmap, note it here first, then reconcile the Roadmap.

**Status legend**: `[ ]` not started · `[~]` in progress · `[x]` done · `[!]` blocked

**Current phase**: Phase 5 complete, ready to start Phase 6

---

## Phase 1 — Metadata + CRUD
- [x] Postgres running via docker-compose (no k8s yet)
- [x] `platform_metadata` schema created: `source_system`, `data_feed`, `schema_registry`, `model_feed`, `model_feed_source`, `run_audit_log`
- [x] Streamlit CRUD app scaffolded (`frontend/`, uv workspace member)
- [x] CRUD pages: `source_system`, `data_feed`, `model_feed`
- [x] **Verify**: create/edit rows for each entity via the app — browser-driven (Playwright) create/edit/delete flow run against all three pages, cross-checked directly against Postgres (not just UI text) for `source_system` → `data_feed` → `model_feed`, including FK correctness, default values (`scd_type=2`, `surrogate_key_column=_scd_id`), and FK-protected delete (deleting a referenced `source_system` correctly fails)

Notes / deviations:
- Delete is a real `DELETE` (not a soft-delete via `is_active`), relying on Postgres FK constraints (default `RESTRICT`) to block deleting a `source_system`/`data_feed` that still has dependent rows. The UI surfaces the resulting DB error rather than preventing the action pre-emptively.
- `model_feed.deletions_enabled=true` is validated in the UI at save time against the linked `data_feed.extraction_type == 'full'`, per the Roadmap's "Model Layer: SCD Design" section — not a DB constraint.
- Bugs found and fixed during implementation (none were roadmap/design issues, all straightforward coding bugs): (1) `try/except/elif` is invalid Python syntax — restructured as nested `if` inside `else`; (2) `uv sync` at the repo root doesn't pull in workspace member dependencies by default — must use `uv sync --all-packages`; (3) SQLAlchemy `text()` mishandles a `::jsonb` cast stuck directly to a named bind parameter — switched to `cast(:param as jsonb)`; (4) `st.dataframe` renders via canvas (glide-data-grid), so UUID columns returned as `uuid.UUID` objects serialize as unreadable byte-dicts — `fetch_table` now stringifies UUID columns before display.

---

## Phase 2 — kind cluster + local storage
- [x] Single-node kind cluster config (`platform/kind/kind-cluster.yaml`) with `extraMounts` → `./data-lake`
- [x] Namespaces created: `metadata`, `orchestration`, `processing`, `query-engine`, `frontend`
- [x] Postgres moved into-cluster (StatefulSet, `metadata` namespace)
- [x] **Verify**: a debug pod writes a file under `/data-lake/raw/...`, visible on host filesystem — confirmed via `kubectl run` writing through a `hostPath` mount, then read back directly from `./data-lake/raw/` on the host

Notes / deviations:
- `platform/kind/kind-cluster.yaml` also adds `extraPortMappings` (host `5432` → node `30432`) paired with a `NodePort` Postgres Service (not in the original Roadmap wording) — this keeps `localhost:5432` working unchanged for host-side tools (the Phase 1 Streamlit app, a local `psql` client) even though Postgres now runs in-cluster. Re-ran the full Phase 1 CRUD verification against the in-cluster Postgres with zero app changes required — confirms this decision worked.
- Roadmap's `platform/storage/` (PV/PVC + StorageClass) was **not** built as literal PV/PVC objects. A PersistentVolumeClaim binds 1:1 to one PV, but the data-lake needs to be readable/writable by many pods across many namespaces (Trino, Spark, Polaris, Dagster ops in later phases) — PVCs are also namespace-scoped, so they can't be shared across namespaces anyway. Used a direct `hostPath` volume (`/data-lake`, backed by the kind node's `extraMounts`) in each pod spec instead, which is the standard pattern for this exact scenario in local/kind clusters. Postgres's own data directory *is* a real single-consumer PVC, via `volumeClaimTemplates` on the StatefulSet (kind's built-in `standard` / local-path-provisioner StorageClass, no custom StorageClass needed).
- `scripts/bootstrap_kind.sh` generates the `postgres-init-scripts` ConfigMap from `metadata/db/init/` via `kubectl create configmap --from-file --dry-run=client -o yaml | kubectl apply -f -` rather than hand-duplicating the SQL into a static manifest, so `01_platform_metadata.sql` has one source of truth.
- Postgres credentials are a plaintext `Secret` (`metadata/k8s/secret.yaml`), matching `.env.example` — fine for a single-user local learning cluster, explicitly not how this would be done against a real cluster (noted in a comment in the file).
- The Phase 1 docker-compose Postgres was stopped (`docker compose down`) to free host port 5432 for the kind NodePort mapping. `docker-compose.yml` is left in place as a still-valid non-k8s quick-start path, but the in-cluster Postgres is now the source of truth going forward.

---

## Phase 3 — Apache Polaris + Trino, manual MERGE proof
- [x] `polaris_db` provisioned in the shared Postgres instance
- [x] Polaris deployed (Postgres-backed via `relational-jdbc`)
- [x] MinIO deployed (S3-compatible object storage, `lakehouse` bucket) — **not in the original plan**, added mid-phase after `FILE` storage type proved unworkable (see Notes)
- [x] Trino deployed with `iceberg.properties` (REST catalog config, S3/MinIO variant, legacy Hadoop-S3A filesystem)
- [x] `S3`-type Polaris catalog registered against MinIO (not the originally-planned `FILE` type)
- [x] Table created and seeded in `clean` via Trino (not hand-written Iceberg files — created through Trino SQL, which is what actually exercises the Trino+Polaris+MinIO write path)
- [x] **Verify**: manual `SELECT` and `MERGE INTO staging` succeed from Trino — confirmed correct upsert semantics (existing row updated, new row inserted, untouched rows left alone)

Notes / deviations:
- **`FILE` storage type abandoned.** Polaris 1.6.0's `FILE` storage type — the reason Polaris was originally chosen over Lakekeeper/Nessie/Unity Catalog OSS — turned out to be gated behind multiple layered "insecure, must not be used" checks: a hard startup-abort production-readiness check (`SUPPORTED_CATALOG_STORAGE_TYPES` containing `FILE`) and a separate request-time gate (`ALLOW_INSECURE_STORAGE_TYPES`) that together resisted every documented bypass tried (env vars, mounted `application.properties`, debug logging to trace the resolution). Pivoted to `S3` storage type backed by a self-hosted MinIO instance instead — fully local, no cloud account, and Polaris's well-supported, non-defense-gated path. `data-lake/clean`, `data-lake/staging`, `data-lake/model`, `data-lake/iceberg` (the local folders created for the `FILE`-storage plan) are now vestigial — actual Iceberg table data lives in MinIO's `lakehouse` bucket, not on the host filesystem.
- **New component**: `query-engine/minio/` (Secret, PVC, Deployment, Service, bucket-creation Job). MinIO is ClusterIP-only (no host port mapping needed) — only Polaris and Trino talk to it, and both are in-cluster.
- **Polaris + MinIO config gotchas** (see Roadmap.md "Iceberg-specific caveats" for the full list): `S3` storage type needs a dummy `roleArn` even for MinIO; `storageConfigInfo` fields (`pathStyleAccess`, `stsUnavailable`) only reliably apply at catalog **creation**, not via `PUT` update, and there's no CLI-supported update path either — confirmed still true in Phase 4 cleanup, delete+recreate remains correct for storage config changes. Polaris's credential-vending path for staged table creation (`CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION`) and table purges (`DROP_TABLE_WITH_PURGE`) were rejected for every principal including root on this version — **resolved in Phase 4 cleanup**: both were a missing RBAC grant (`TABLE_WRITE_DATA`, only satisfied by `CATALOG_MANAGE_CONTENT`, which our `catalog_admin` role never had — traced via the exact `apache-polaris-1.6.0` authorization source, not assumption), not a deliberate CVE lockdown. Fixed via `polaris privileges catalog grant ... CATALOG_MANAGE_CONTENT`. Re-enabling `vended-credentials-enabled=true` on Trino still doesn't work after that fix, but for an unrelated reason — the catalog's `stsUnavailable: true` means Polaris has no real credentials to vend at all (`Credential vending was requested ..., but no credentials are available`) — so `vended-credentials-enabled=false` stays as permanent, correct config for a MinIO-backed catalog, not a pending-fix workaround. **The actual root cause of the core blocker**: Polaris's own server-side `S3FileIO` client (independent of anything Trino sends) wasn't picking up the catalog's `s3.endpoint` property and was defaulting to real AWS S3 — confirmed by reading Polaris's own logs and by capturing live MinIO traffic showing the failing request never arrived. Fixed by setting `AWS_ENDPOINT_URL_S3` (plus `AWS_REGION`/`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`) directly as env vars on the **Polaris** deployment — a separate credential path from Trino's own catalog config.
- A suspected Trino bug (trinodb/trino#25187, path-style-access not honored with Iceberg REST + S3) turned out to be a red herring — the real fix was Polaris-side. Trino was left on the legacy Hadoop-S3A filesystem (`fs.hadoop.enabled` + `hive.s3.*`) rather than the modern `fs.native-s3` at the time, since that was the config already in place when the actual root cause was found. **Re-tested and reverted in Phase 4 cleanup**: switched back to `fs.native-s3` + `s3.*` properties, confirmed working (read, create, insert, select, full `dbt build` all green) — the legacy filesystem config is no longer used.
- One orphaned test table (`iceberg.clean.customers5`), previously undroppable due to the `DROP_TABLE_WITH_PURGE` permission gate above, was dropped in Phase 4 cleanup once that gate was resolved.

---

## Phase 4 — dbt clean→staging
- [x] dbt project scaffolded (`dbt/data_platform/`, uv workspace member)
- [x] `clean` registered as a dbt source (`models/staging/_sources.yml`)
- [x] `trino__current_timestamp()` precision-6 macro override added
- [x] Shared `row_hashes.sql` macro built — `row_hash(columns)` generates a stable hash from a column list, used for both `_key_hash` (business key) and `_attr_hash` (non-key columns)
- [x] `stg_customers.sql` incremental merge model built: hash-gated anti-join excludes unchanged rows *before* the merge (`unique_key='_key_hash'`), rather than relying on the merge's `WHEN MATCHED` to no-op
- [x] **Verify**: idempotent re-runs produce zero writes when source data is unchanged — confirmed via `iceberg.staging."customers$snapshots"` snapshot count staying at 5 across a no-op re-run (not just "same end result", genuinely zero new Iceberg snapshots)
- [x] **Verify**: changed scenario — `UPDATE` on one existing row + `INSERT` of a new row in `clean.customers`, re-run produced `MERGE (2 rows)`, and `staging.customers` correctly reflected the update-in-place and the new row, with untouched rows unchanged

Notes / deviations:
- **Blocked, then resolved: Polaris `DROP_WITH_PURGE_ENABLED`.** dbt-trino's `merge` incremental strategy creates and drops a temp view (`customers__dbt_tmp`) every run; Polaris rejected the `DROP ... purge` with `Unable to purge entity ... set the Polaris configuration DROP_WITH_PURGE_ENABLED or the catalog configuration polaris.config.drop-with-purge.enabled`. Four env-var-based attempts at the realm-level `POLARIS_FEATURES_DROP_WITH_PURGE_ENABLED` flag all had zero effect — the error message names two *different* mechanisms, and only the second (`polaris.config.drop-with-purge.enabled`, a **per-catalog property** on the catalog entity itself, not a realm/Quarkus setting) actually applies once a catalog already exists. Setting it via a hand-rolled `curl PUT` to the Management API also silently failed to persist. **Fix**: set it via the official Polaris CLI (`apache-polaris` on PyPI, run via `uvx --from apache-polaris polaris ... catalogs update --set-property polaris.config.drop-with-purge.enabled=true data_platform`) — persistence confirmed via `catalogs get` (`entityVersion` incremented). This also revises the Phase 3 note that catalog `PUT` updates are fundamentally unreliable: in hindsight that was very likely hand-rolled `curl` requests not constructing the update request correctly (missing `currentEntityVersion`/full property merge), not a genuine Polaris server bug — the CLI updates a live catalog correctly. `register-catalog.sh` now includes `polaris.config.drop-with-purge.enabled` in the catalog's creation-time properties so a fresh bootstrap doesn't need the follow-up CLI step at all. Full writeup in [Learnings.md](Learnings.md).
- This resolves the `DROP_TABLE_WITH_PURGE` rough edge noted as "unresolved" in Phase 3 for dbt's use case (dropping the temp *view*, which only needs the catalog-level property fix above). It turned out there's a second, separate gate for purging a real *table* — a missing RBAC privilege grant — fixed separately during Phase 4 cleanup; see the Phase 3 notes above and Learnings.md.

---

## Pre-Phase-5 note: `polaris_client` module

Built ahead of Phase 5 proper (see Learnings.md for the full writeup): `query-engine/polaris_client/`, a new uv workspace member wrapping `apache_polaris.sdk.management.PolarisDefaultApi` (the real generated SDK the `polaris` CLI itself is built on) with the specific catalog/privilege operations this project has used so far. `register-catalog.sh` now delegates to it (`polaris_client.bootstrap`) instead of driving the CLI directly. Verified via a full live smoke test (create/exists/get/update/grant/list/revoke/delete) against a disposable catalog before trusting it against the real one — all steps passed. Exists so Dagster (Phase 5+) has a proper typed client to import for any future catalog-aware work (e.g. scheduled Iceberg table maintenance), rather than shelling out to a CLI subprocess from an orchestrator.

---

## Phase 5 — Dagster wiring (stubbed extraction)
- [x] `orchestration/dagster_data_platform/` scaffolded (uv workspace member: dagster, dagster-webserver, dagster-dbt, dagster-k8s, dagster-postgres, dbt-core, dbt-trino)
- [x] `dagster dev` running locally with `K8sRunLauncher` configured for op pods, Postgres-backed instance storage (`dagster_db`, third logical DB in the shared Postgres instance)
- [x] `dagster-dbt` loads the Phase 4 dbt project as assets (`@dbt_assets`, `DbtProject`/`DbtCliResource`)
- [x] Stub `landing_customers`/`raw_customers`/`clean_customers` assets feed the real dbt staging asset — connected as one lineage graph via a custom `DagsterDbtTranslator` mapping the `clean.customers` dbt source onto the same asset key the stub produces, not just coincidentally-ordered separate chains
- [x] `run_audit_log` writes wired into asset execution — start/finish rows for all four layers (`landing`, `raw`, `clean`, `staging`), including `dagster_run_id` and row counts
- [x] **Verify**: triggered via `dagster job launch` (through the real `QueuedRunCoordinator` → daemon → `K8sRunLauncher` path, not the in-process `dagster asset materialize` shortcut) — confirmed a real Kubernetes `Job`/pod launched in the `orchestration` namespace and ran to `Completed`, confirmed `DagsterRunStatus.SUCCESS`, confirmed all four `run_audit_log` rows with correct status/row counts, confirmed `iceberg.staging.customers` reflects the run's data

Notes / deviations:
- **New component**: `orchestration/Dockerfile` — the image `K8sRunLauncher` launches run pods from. Repo-root build context (`docker build -f orchestration/Dockerfile .`) so the workspace lockfile and `dbt/data_platform` are both available; includes a build-time `dbt parse` to bake `target/manifest.json` into the image, since `DbtProject.prepare_if_dev()` is a no-op outside the `dagster dev` CLI context and the launched pod never runs through that.
- **New component**: `orchestration/dagster_home/dagster.yaml` (local instance config) and an identical `dagster-instance` ConfigMap in the `orchestration` namespace (mounted into every launched pod by `K8sRunLauncher`'s `instance_config_map` config) — same templated content (`hostname: {env: POSTGRES_HOST}`), resolved against different values in each place, since the local `dagster dev` process and an in-cluster run pod reach the same shared Postgres instance via different hostnames (`localhost` NodePort vs. `postgres.metadata.svc.cluster.local`). Full reasoning in Learnings.md.
- **Scope decision, not a workaround**: a single `orchestration` image contains both the Dagster code *and* dbt-core/dbt-trino/the dbt project, rather than the fully-separated `orchestration` + `dbt` images the Roadmap's repo structure eventually envisions. dagster-dbt invokes dbt via CLI subprocess from within the same process either way, so one image is sufficient for what Phase 5 needs; splitting them is a legitimate future refinement if a concrete reason to (image size, independent versioning) shows up, not something skipped for lack of effort.
- Several real gotchas hit and documented in Learnings.md: a corrupted `uv` cache/venv install (recurred four times; fixed reliably only by `uv cache clean` + rebuilding `.venv` from scratch, not `--reinstall-package`), `dagster-dbt`/`dagster-k8s`/`dagster-postgres` using a different version scheme than `dagster` itself, `DbtProject` needing its own `profiles_dir` separate from `DbtCliResource`'s, `dagster asset materialize` never touching the run launcher at all (use `dagster job launch`), the default `QueuedRunCoordinator` needing the daemon running to launch anything, and `K8sRunLauncher.service_account_name` being required by the Python constructor despite the config schema marking it optional.
- **Added after a concurrency question, not originally in scope for this phase**: our `clean_customers` stub's `DELETE`+`INSERT` (two separate Iceberg commits, not one atomic operation) is genuinely unsafe under two concurrent runs of the same feed — confirmed by checking Iceberg's actual optimistic-concurrency model rather than assuming. Fixed at the Dagster level with **concurrency pools**, not a hand-rolled lock table: `FEED_POOL = f"feed:{FEED_CODE}"` applied to all four of the feed's assets, `concurrency.pools.default_limit: 1` + `run_monitoring.free_slots_after_run_end_seconds: 300` in `dagster.yaml` (the latter to stop a crashed run's stale pool claim from permanently blocking everything after it). Also collapsed the repeated `start_run`/`finish_run` boilerplate into one `PostgresMetadataResource.log_ingestion_step()` context manager, used by all four assets. **Verified concretely**: launched two runs of the same feed back-to-back, confirmed via `kubectl get pods` and `DagsterRunStatus` that only one ever executed at a time (`STARTED` vs `QUEUED`), confirmed the queued run picked up automatically once the first finished with zero overlap (`run_audit_log` timestamps 10+ seconds apart), both runs completed `SUCCESS` with correct audit rows.

---

## Phase 6 — Spark Operator + real raw→clean
- [ ] `spark-operator` (Kubeflow) deployed to `processing` namespace
- [ ] `processing/raw_to_clean/` PySpark job implemented (uv workspace member)
- [ ] Schema validation against `schema_registry` implemented
- [ ] Stub `clean` asset replaced with real `SparkApplication` CR launched from a Dagster op
- [ ] **Verify**: trigger a materialization, confirm pods launch in kind, confirm `run_audit_log` rows written with correct status/row counts

Notes / deviations:

---

## Phase 7 — Model layer: Type 1/Type 2 dims + facts
- [ ] Unified technical columns (`_scd_id`, `_valid_from`, `_valid_to`, `_updated_at`, `_is_deleted`) defined via `snapshot_meta_column_names`
- [ ] At least one `scd_type=2` dimension: dbt snapshot, `check` strategy
- [ ] At least one `scd_type=1` dimension: plain incremental `merge` model, update-in-place
- [ ] `int_<feed>_with_deletes.sql` intermediate model for `deletions_enabled` feeds
- [ ] At least one incremental fact model joined to a dimension `_scd_id`
- [ ] **Verify (Type 2)**: simulated attribute change produces a new version with correct `_valid_from`/`_valid_to`/`_scd_id`
- [ ] **Verify (Type 2)**: simulated deletion (full-load feed) produces an `is_deleted=true` new version via the synthetic-update path
- [ ] **Verify (Type 1)**: same two scenarios update the existing row in place, no new row created

Notes / deviations:

---

## Phase 8 — Serve layer
- [ ] Python codegen step reads `model_feed` and renders `_latest.sql`/`_historical.sql` templates
- [ ] Generated views land in `dbt/models/serve/generated/`
- [ ] Codegen wired as a Dagster asset upstream of `dbt build`, downstream of model-layer assets
- [ ] Type 1 tables correctly collapse "latest" and "historical" to the same view
- [ ] **Verify**: `dbt build` + `dbt test` green for generated serve views

Notes / deviations:

---

## Phase 9 — End-to-end hardening
- [ ] One real source connected (REST API or CSV drop) end-to-end through all layers
- [ ] Dagster schedules/sensors configured
- [ ] Full watermark handling verified for both `data_feed` (source extraction) and `model_feed` (staging→model merge) scopes
- [ ] **Verify**: full pipeline run succeeds against the real source
- [ ] **Verify**: deliberate mid-pipeline failure, then confirm re-run picks up cleanly from `run_audit_log` state

Notes / deviations:

---

## Phase 10 — (Stretch) Azure config swap proof
- [ ] Real ADLS Gen2 account provisioned
- [ ] Trino `iceberg.properties` updated (`fs.azure.enabled=true` + `azure.oauth.*`)
- [ ] Polaris catalog storage config re-registered from `FILE` to `AZURE`
- [ ] **Verify**: pipeline runs end-to-end against Azure storage with no dbt/model changes

Notes / deviations:
