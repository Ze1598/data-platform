# Learning Data Platform: dbt + Kubernetes + Iceberg — Roadmap

## Context

This is a greenfield learning project to build a small but architecturally realistic data platform: dbt-driven transformations over Apache Iceberg tables, orchestrated by Dagster, running on Kubernetes. The goal is hands-on experience with the real mechanics of a lakehouse platform — not a toy — while keeping initial cost/complexity down by running entirely locally (a kind cluster, local filesystem storage) with a deliberate design so the storage layer can later be repointed at real Azure Storage (ADLS Gen2) via config change, not a rewrite.

## Architecture Decisions

- **Compute engine**: Trino (via `dbt-trino`) for all SQL transformations over **Apache Iceberg** tables. A separate Spark job (Kubeflow `spark-operator`) handles Python-based extraction/parsing/validation work, invoked directly by the orchestrator — not as native dbt Python models, since Trino doesn't support those.
- **Table format**: **Apache Iceberg**, chosen over the originally-planned Delta Lake. Trino's Delta connector only supports Hive Metastore (Thrift) or Glue as a catalog backend — there's no REST catalog option, because Delta never developed an open, multi-vendor REST catalog protocol the way Iceberg did. The whole ecosystem has converged on Iceberg's REST Catalog spec since 2023 (Apache Polaris, Unity Catalog OSS, AWS S3 Tables, Google BigLake, even Databricks' own Unity Catalog as of mid-2025). Trino's Iceberg connector is also one of its most mature — full `MERGE`/`INSERT`/`UPDATE`/`DELETE` support for spec v2+ tables — versus an unclear write story for Delta via Unity Catalog's UniForm layer. Switching avoids Hive Metastore's operational weight (Thrift service, schema migrations, version-compatibility matrix) for a lighter, spec-standard REST catalog.
- **Catalog**: **Apache Polaris** (`apache/polaris`, ASF top-level project as of Feb 2026), originally chosen over Lakekeeper/Nessie/Unity Catalog OSS partly *because* its `FILE` storage-config type could point straight at local disk without needing an extra storage component. That plan didn't survive contact with the actual server: Polaris 1.6.0's `FILE` storage type turned out to be gated behind multiple layered "insecure, must not be used" checks (a hard startup-abort production-readiness check, a request-time `ALLOW_INSECURE_STORAGE_TYPES` gate) that resisted every documented bypass. **Pivoted mid-Phase-3 to `S3` storage type backed by MinIO** (self-hosted, S3-compatible, still fully local — see "Object storage" below) — Polaris's well-supported, non-defense-gated path. Persists via a `relational-jdbc` connection to Postgres, reusing the shared Postgres instance.
- **Object storage**: **MinIO**, self-hosted S3-compatible object storage running in-cluster (own PVC). Not in the original plan — added specifically to give Polaris's catalog a storage backend that isn't defense-gated the way `FILE` turned out to be. Fully local (no cloud account, no external network calls); Azure Blob Storage is the eventual Phase 10 target, and building against MinIO's S3 API now is arguably a *better* rehearsal for that than raw filesystem paths would have been, since ADLS Gen2 is also an object-store API, not a POSIX filesystem.
- **Orchestrator**: Dagster, with `dagster-dbt` loading the dbt project as native assets, and the Kubernetes run launcher so materializations run as real pods.
- **Kubernetes target**: a local, single-node kind cluster. Single-node specifically because Trino's local-filesystem connector requires the storage path to be shared across all cluster nodes — trivially true with one node.
- **Storage**: two-tier now, not one. `landing`/`raw` (plain files, never Iceberg tables) live on local filesystem (`./data-lake/{landing,raw}`, mounted into kind via `extraMounts`). `clean`/`staging`/`model` (Iceberg tables) live in MinIO's `lakehouse` bucket, addressed as `s3://lakehouse/<schema>/<table>/...` — **not** under the local `data-lake/` mount at all. `data-lake/clean`, `data-lake/staging`, `data-lake/model`, `data-lake/iceberg` are vestigial leftovers from the abandoned `FILE`-storage plan; harmless to leave, safe to delete. Designed to swap to real Azure services later: `landing`/`raw` to ADLS Gen2 via `abfss://`, `clean`/`staging`/`model` to ADLS Gen2 via Polaris's `AZURE` storage-config type (parallel to how MinIO/S3 works today).
- **Python tooling**: `uv` everywhere — a root `uv` workspace, one member per deployable Python component, Dockerfiles built on the `uv` base image with `uv sync --frozen`.
- **Client connectivity (JDBC vs. ADBC)**: the metadata layer (Polaris ↔ Postgres) is transactional, small-record CRUD — JDBC (via Polaris's `relational-jdbc` persistence) is the right fit and is what Polaris's Java/Quarkus stack is built on. `dbt-trino`'s transformation SQL (merge/incremental/snapshot) is execution-oriented — it sends a statement and gets back an ack/row count, not a bulk result set — so it stays on the standard JSON-based `trino-python-client`; there's no large columnar payload for ADBC to accelerate there. The one place ADBC genuinely fits is **Streamlit pulling large query results from the serve layer into dataframes for visualization** — a real, actively-maintained ADBC driver for Trino exists (ADBC Driver Foundry) and is the intended connectivity path for `frontend/db.py`, not the plain `trino` client.

### Iceberg-specific caveats to carry into implementation
- `dbt-trino`'s `merge` incremental strategy and `snapshot_merge_sql` (our Type 2 mechanism) are connector-agnostic — no Iceberg-specific rewrite needed for the merge/snapshot logic itself.
- dbt's default `current_timestamp` macro renders `TIMESTAMP(3)` (millisecond precision); Iceberg only supports microsecond precision, so Trino can't write `timestamp(3)` into Iceberg tables. This breaks the `_valid_from`/`_updated_at` columns on Type 2 snapshots unless the project overrides `trino__current_timestamp()` to `current_timestamp(6)` **before** the first snapshot run.
- Azure portability is config-only but two-sided, not a single property flip: (1) Trino's `iceberg.properties` catalog file swaps its `hive.s3.*`/S3 properties for `fs.azure.enabled=true` + `azure.oauth.*` credentials, and (2) Polaris's own catalog storage config (a one-time registration action via its REST API, not a code change) switches `storageConfigInfo.storageType` from `S3` to `AZURE` (`default-base-location=abfss://...`, tenant/app credentials). No dbt models or snapshot definitions change either way.
- **Polaris + MinIO gotchas found the hard way (Phase 3), worth knowing before touching this config again**:
  - Polaris's `S3` storage type requires a `roleArn` even against MinIO, which has no real IAM/STS — any syntactically-valid dummy ARN works (`arn:aws:iam::000000000000:role/minio-polaris-role`), and `storageConfigInfo.stsUnavailable=true` tells Polaris not to attempt an actual STS assume-role call.
  - `storageConfigInfo` fields (`pathStyleAccess`, `stsUnavailable`) only reliably apply at catalog **creation** (`POST`) — a `PUT` update to an existing catalog silently drops them (confirmed live: response echoes `pathStyleAccess: false` regardless of what was sent). Delete and recreate the catalog rather than trying to update it.
  - Polaris's REST catalog protocol vends short-lived storage credentials to query engines by default (`iceberg.rest-catalog.vended-credentials-enabled=true` on the Trino side) — but the specific operation used during staged table creation (`CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION`) is rejected for every principal, including root/`service_admin`, on this Polaris version (a permission tightened by a recent CVE fix). Set `vended-credentials-enabled=false` and give Trino MinIO's static credentials directly (`hive.s3.aws-access-key`/`hive.s3.aws-secret-key`) instead. The same pattern applies to `DROP TABLE` (`DROP_TABLE_WITH_PURGE` is likewise rejected) — a currently-unresolved rough edge to watch for if dbt ever issues a full-refresh drop-and-recreate.
  - **The actual blocker, and the one that cost the most time**: Polaris's own server-side `S3FileIO` client (used to validate/finalize table commits, independent of anything Trino does) does not pick up the catalog's `s3.endpoint` property at all — it defaults to real AWS S3 and fails with a `301 must be addressed using the specified endpoint` error, confirmed via Polaris's own logs plus a live MinIO traffic capture showing the failing request never reaching MinIO. Fix: set `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and — the missing piece — `AWS_ENDPOINT_URL_S3` (the AWS SDK Java v2 env var for a custom S3 endpoint, supported since SDK 2.28.1) directly on the **Polaris** deployment. This is a separate credential/endpoint path from whatever Trino is configured with.
  - Trino's catalog config ended up on the legacy Hadoop-S3A filesystem (`fs.hadoop.enabled` + `hive.s3.*` properties) rather than the modern `fs.native-s3`, because that was the config in place when the real (Polaris-side) root cause was finally found and confirmed working. `fs.native-s3` was abandoned earlier due to a suspected Trino bug (trinodb/trino#25187) that, in hindsight, probably wasn't the actual cause here — revisiting `fs.native-s3` now that `AWS_ENDPOINT_URL_S3` is set on Polaris is a plausible future cleanup, not yet re-tested.

## Layer Model

- **landing**: raw file drops (uploads, API responses) before entering the platform. Own storage root, isolated for security purposes (own "account" conceptually, even though today just another local folder).
- **raw**: pure raw data. Each run writes its own watermarked folder (`raw/<source_system>/<data_feed>/run_id=.../`) — plain files (parquet/csv/json), **not** an Iceberg table (doesn't fit a transaction-log/snapshot table model).
- **clean**: cleaned/parsed/schema-validated output of a single run. Iceberg format, but a **snapshot per run, not cumulative** — each run overwrites clean's content for that feed.
- **staging**: cumulative **upsert** Iceberg layer. Each run's clean snapshot is merged into staging using the shared hash-based pattern (see "Model Layer: SCD Design" below) — `ON target._key_hash = source._key_hash`, `UPDATE` only fires when `_attr_hash` actually differs — so staging holds the latest known state per key across all runs, and an unchanged row from `clean` produces zero writes.
- **model**: Kimball facts + dimensions. Dimensions are configurable **Type 1 (overwrite) or Type 2 (versioned)** per `model_feed`, with updates and deletions each independently toggleable — see "Model Layer: SCD Design" below. Facts are incremental merge/insert models joined to dimension surrogate keys.
- **serve**: views over model — a **latest** (current-state) and **historical** (full version history, Type 2 only) view per model table, generated automatically, not hand-authored per table.

**dbt project scope**: dbt owns **clean → staging → model → serve**. landing → raw → clean (extraction, parsing, schema validation) is custom Python/Spark code orchestrated by Dagster, since raw isn't a clean queryable Iceberg source. `clean` is registered as a dbt source.

**Serve layer approach**: a small Python codegen step (Dagster op, runs before `dbt build`) reads `model_feed` metadata and renders Jinja-templated `_latest.sql`/`_historical.sql` dbt view models into `dbt/models/serve/generated/`. This keeps every generated view a first-class dbt model with lineage/tests, while still being fully metadata-driven (no per-table hand authoring) — better than a bare `run-operation` macro (which `dagster-dbt` can't represent as individual assets) or bypassing dbt entirely via a raw Trino-client op (loses dbt lineage/testing). For Type 1 model tables, "latest" and "historical" collapse to the same view (no history is kept).

## Model Layer: SCD Design (Type 1 vs Type 2, Updates, Deletions)

Every model-layer table — facts, Type 1 dimensions, Type 2 dimensions — carries the same seven technical columns, aligned by name across both mechanisms via dbt snapshot's `snapshot_meta_column_names` config, so Type 1 and Type 2 tables are structurally identical and differ only in how the columns get populated:

- `_key_hash` — a hash of the business key column(s), computed by a shared macro. Used as the match/join condition for every merge in the platform (clean→staging, Type 1, Type 2's snapshot `unique_key`) — a single-column comparison regardless of whether the underlying business key is one column or five.
- `_attr_hash` — a hash of the columns that should trigger a change when they differ (see "Hash-based change detection" below — the source column list differs slightly between staging and the model layer). Gates whether a merge's `UPDATE` branch does anything at all, and is the sole `check_cols` value for Type 2 snapshots.
- `_scd_id` — version-specific surrogate key; what fact tables join against for point-in-time correctness on Type 2 dimensions.
- `_valid_from`, `_valid_to` — version lifecycle (`_valid_to IS NULL` = current row). Type 1 tables always have `_valid_to = NULL` — no history is kept, so it's structurally present but never populated.
- `_updated_at` — last time the row was touched.
- `_is_deleted` — boolean. Ordinary tracked data, not a platform-inferred signal — see deletion mechanism below.

**Hash-based change detection, reused identically everywhere a merge happens** (clean→staging, Type 1 dimensions, Type 2 snapshots, mutable facts): a shared macro computes `_key_hash` and `_attr_hash` for any model from metadata, so there's no per-table hand-written hashing or comparison logic — just a matter of telling the macro which columns are keys and which aren't. Two different "non-key" sources feed `_attr_hash` depending on the layer, and it's worth being precise about which: at **staging**, `_attr_hash` is computed from *every* column except `data_feed.business_key_columns` (staging just wants to know "did anything about this record change," full stop). At the **model layer**, `_attr_hash` is computed from the explicitly curated `model_feed.tracked_columns` plus `is_deleted` — a deliberately narrower set, since which attribute changes should actually trigger a new SCD2 version is a modeling decision, not "anything in the row." No new metadata fields are needed for any of this — it's a new way of *using* `business_key_columns`/`tracked_columns`, which already exist.

This gating is not just a tidiness improvement — it's what makes "idempotent" mean "zero writes for unchanged rows," not just "same end result." Without it, every merge run rewrites every matched row's underlying Iceberg file whether or not anything changed (real, avoidable file churn — see Learnings.md's Iceberg maintenance discussion), and an ungated Type 2 snapshot would spawn a spurious new version on every single run regardless of whether the data actually changed — a correctness bug for SCD2, not just inefficiency.

**`model_feed` carries three flags that together determine merge behavior:**
- `scd_type` (1 or 2) — *how* a change is applied: in place (1) or as a new version (2).
- `updates_enabled` (boolean) — *whether* attribute changes are detected/applied at all. If false, the feed is insert-only: matched rows are left untouched on merge, and `scd_type` is moot.
- `deletions_enabled` (boolean) — *whether* missing-row deletion detection runs. Valid only when the source `data_feed.extraction_type = 'full'` (validated at config-save time, not a DB constraint) — "missing from this load" only implies "deleted" when the load is a complete snapshot, not an incremental slice.

**Implementation per `scd_type`:**
- **Type 2** — a dbt **snapshot**, `check` strategy, `check_cols: ['_attr_hash']`, `unique_key: '_key_hash'`. A change to `_attr_hash` — which folds in `is_deleted` flipping true — closes the current version's `_valid_to` and inserts a new current version; an unchanged `_attr_hash` produces no new version at all. dbt's `hard_deletes` config is deliberately unused (left at default `ignore`): `hard_deletes: new_record` isn't confirmed supported on `dbt-trino`, and it isn't needed anyway since deletion is just another `_attr_hash`-changing update.
- **Type 1** — a plain `incremental` model, `merge` strategy: `MERGE ... ON target._key_hash = source._key_hash WHEN MATCHED AND target._attr_hash != source._attr_hash THEN UPDATE SET <tracked columns>, is_deleted = source.is_deleted, _attr_hash = source._attr_hash, _updated_at = now() WHEN NOT MATCHED THEN INSERT`. One statement, in place, no history, no write at all for rows whose `_attr_hash` hasn't changed.

**Deletion mechanism — a deletion is synthesized as an update, not detected as a special case.** For a `deletions_enabled` feed, an intermediate model (`int_<feed>_with_deletes.sql`) sits between staging and the snapshot/incremental model. It finds business keys present in the model's current rows but absent from this run's full staging load, synthesizes an `is_deleted=true` row for each (attributes carried over from the last known state), and unions those rows into the effective source. From the snapshot/merge's perspective, a deletion is indistinguishable from a normal attribute update — the same single mechanism handles both. This intermediate model exists only for feeds with `deletions_enabled=true`; other feeds read directly from staging.

## Incremental Loading & Watermarks

Two independent watermark scopes:

- **Source extraction** (`data_feed`): `incremental_column`/`incremental_column_type` name the source column driving incremental pulls into `raw`. `last_watermark_value` and `last_run_id` are denormalized onto `data_feed` (mirroring `model_feed`) for fast lookup by the orchestrator; `data_feed_run` remains the full run history (`<stage>_watermark_value_start`/`end` per stage per run).
- **Staging → model merge** (`model_feed`): `watermark_column`/`last_watermark_value` control which staging rows are considered for the next merge, using staging's own technical columns — `_clean_run_id`, `_loaded_at`, `_key_hash`, `_attr_hash`, stamped/computed during the clean→staging upsert — filtering `WHERE staging._loaded_at > model_feed.last_watermark_value`. This optimization only benefits feeds *without* `deletions_enabled`: deletion detection requires comparing the full current staging snapshot against the model's current rows regardless, so deletion-tracking feeds do a full scan every run irrespective of this watermark.

## Metadata Schema (Postgres, `platform_metadata` DB)

- **`source_system`**: id, code, name, description, system_type (database/api/file_drop/saas), connection_config (jsonb — host/base_url/auth method; secret *references* only), is_active, timestamps
- **`data_feed`** (one row per source object/table/endpoint): id, source_system_id (fk), code, name, object_name, extraction_type (full/incremental), incremental_column(+type), extraction_config (jsonb), landing/raw path templates, business_key_columns (jsonb array), staging_table_name, schedule_cron, last_watermark_value, last_run_id (fk), is_active, timestamps
- **`schema_registry`** (versioned expected schema of each feed's clean output, used by raw→clean validation): id, data_feed_id (fk), version, column_definitions (jsonb array of {name, data_type, nullable, ordinal, description}), is_current, effective_from/to — unique `(data_feed_id, version)`, partial unique index for one current row per feed
- **`model_feed`** (fact/dim config): id, code, model_type (fact/dimension), staging_source_data_feed_id (fk), business_key_columns, tracked_columns (change-tracked attribute cols), surrogate_key_column (`_scd_id`), scd_type (1 or 2, default 2), updates_enabled, deletions_enabled, watermark_column, last_watermark_value, last_run_id (fk), is_active, timestamps
- **`model_feed_source`** (bridge table for facts joining >1 staging source): model_feed_id (fk), data_feed_id (fk), role
- **`data_feed_run`** (one row per `data_feed` per job run — extraction + contract-validation concern): run_id (uuid pk), data_feed_id (fk), dagster_run_id, job_started_timestamp/job_ended_timestamp/job_successful, then per stage (landing/raw/clean) a repeated column group: `is_<stage>_successful`, `<stage>_end_timestamp`, `<stage>_error_message`, `<stage>_rows_read/inserted/updated/deleted`, `<stage>_output_path`, `<stage>_watermark_value_start/end` — unique `(data_feed_id, dagster_run_id)`. `job_successful`/`job_ended_timestamp` roll up as each stage finishes (AND-accumulated / always-latest), so they land on the correct final state whether the chain completes or fails partway through.
- **`data_model_run`** (one row per model unit per job run — warehouse-building concern: staging/model/serve): same shape as `data_feed_run` but keyed by `model_key` (text, not an FK — `model_feed` has no real rows yet) + `uses_feeds` (comma-separated `data_feed.code`s this model unit draws from) instead of `data_feed_id`, unique `(model_key, dagster_run_id)`. `staging` lives here rather than on `data_feed_run` even though it's currently built per-feed (`stg_customers`/`stg_sales`), because conceptually it's warehouse-building work, not source extraction — see Learnings.md for the full reasoning.

This same Postgres instance also hosts `polaris_db` for Apache Polaris (the Iceberg REST catalog).

## Kubernetes Hosting Model

One kind cluster for the whole platform — not split across multiple clusters. Modules are separated by **namespace** (`metadata`, `orchestration`, `processing`, `query-engine`, `frontend`), and within a namespace, workloads split by lifecycle, not by module:

- **Long-running services** (Postgres, Apache Polaris, Trino, Dagster webserver+daemon, Streamlit) — `Deployment`/`StatefulSet` + `Service`, always up.
- **On-demand compute** (a dbt run, a Spark extraction) — `Job` or `SparkApplication` CR, launched per run by Dagster's `K8sRunLauncher`, runs to completion, pod disappears. dbt has no server component; it's a CLI invoked inside a pod on demand.

Each module gets exactly one container image it owns (custom-built where the module has its own code; off-the-shelf where it doesn't):

| Module | Image | Workload type |
|---|---|---|
| `metadata` | official `postgres` + init SQL via ConfigMap | StatefulSet |
| `query-engine` | official `trino` (Helm chart) + `apache/polaris` (Postgres-backed via `relational-jdbc`) + `minio/minio` (S3-compatible object storage), all config-driven (no custom build) | Deployment x3 |
| `orchestration` | custom-built (Dagster + `dagster-dbt` + `dbt-core`/`dbt-trino`) | Deployment (webserver/daemon) + Jobs (per-run op pods) |
| `dbt` | custom-built (dbt project + deps), image consumed by `orchestration`'s op pods | no standalone deployment — runs inside orchestration-launched Jobs |
| `processing` | custom-built (PySpark job code) | `SparkApplication` CR (via `processing`'s spark-operator, itself off-the-shelf) |
| `frontend` | custom-built (Streamlit, using the Trino ADBC driver for serve-layer reads) | Deployment |

## Repo Structure (module-first — each module owns its code, Dockerfile, and k8s manifests)

```
data-platform/
  pyproject.toml, uv.lock          # uv workspace root
  Makefile, README.md, .env.example
  Roadmap.md                       # this file
  platform/                        # cluster-wide concerns, not owned by one module
    kind/kind-cluster.yaml         # single-node, extraMounts -> ./data-lake
    namespaces/                    # metadata.yaml, orchestration.yaml, processing.yaml, query-engine.yaml, frontend.yaml
    storage/                       # data-lake PV/PVC + StorageClass definitions
  metadata/                        # module: platform config DB (source_system/data_feed/model_feed/data_feed_run/data_model_run)
    db/init/                       # 01_platform_metadata.sql, 02_polaris_db.sql
    k8s/                           # postgres StatefulSet, Service, PVC (namespace: metadata)
  query-engine/                    # module: Iceberg query layer
    trino/                         # Helm values: iceberg.properties (REST catalog config, S3/MinIO today, Azure variant later)
    polaris/                       # Deployment/Service/Secret manifests, bootstrap-job.yaml (schema init), register-catalog.sh (namespace: query-engine)
    minio/                         # Deployment/Service/PVC/Secret, bucket-creation Job (namespace: query-engine)
    k8s/
  dbt/data_platform/                # module: dbt calculations (clean -> staging -> model -> serve)
    dbt_project.yml, profiles/profiles.yml
    models/staging/                # _sources.yml (clean as source), stg_<feed>.sql (incremental merge)
    models/model/intermediate/     # int_<feed>_with_deletes.sql (deletions_enabled feeds only)
    models/model/dimensions_type1/ # scd_type=1 dims: plain incremental merge, update-in-place
    models/model/facts/            # fct_<x>.sql
    models/serve/generated/        # codegen output ("latest"/"historical" per model_feed; collapse to one for Type 1)
    snapshots/dim_<x>_snapshot.sql # scd_type=2 dims: dbt snapshot, check strategy, snapshot_meta_column_names aligns columns with Type 1
    macros/                        # trino__current_timestamp.sql (precision-6 override); row_hashes.sql (shared _key_hash/_attr_hash generator, metadata-driven — see "Model Layer: SCD Design")
    Dockerfile                     # uv-based image consumed by orchestration Jobs
  orchestration/                   # module: Dagster
    dagster_data_platform/         # uv workspace member (dagster, dagster-dbt, dbt-core, dbt-trino)
      definitions.py
      assets/ (landing_, raw_, clean_ [invokes Spark], dbt_assets.py, serve_codegen_asset.py)
      resources/ (trino_resource.py, postgres_metadata_resource.py, spark_k8s_resource.py)
    Dockerfile                     # uv-based
    k8s/                           # webserver/daemon Deployment, user-code Deployment, K8sRunLauncher RBAC (namespace: orchestration)
  processing/                      # module: Spark extraction/validation jobs
    raw_to_clean/                  # uv workspace member
      main.py, transformations/
    Dockerfile
    k8s/                           # spark-operator Helm values, SparkApplication templates (namespace: processing)
  frontend/                        # module: Streamlit CRUD + viz
    app.py, pages/ (source_systems, data_feeds, model_feeds, run_history), db.py  # db.py uses the Trino ADBC driver for serve-layer reads
    Dockerfile                     # uv workspace member
    k8s/                           # Deployment, Service (namespace: frontend)
  scripts/                         # uv workspace member — bootstrap_kind.sh, load_images.sh, seed_metadata_db.py
  data-lake/                       # host-mounted into kind: landing/ raw/ actively used; clean/ staging/ model/ iceberg/ vestigial (Iceberg tables now live in MinIO's `lakehouse` bucket, not here)
  tests/integration/
```

## Phased Build Order (each phase independently runnable/testable)

1. **Metadata + CRUD** — Postgres (docker-compose, no k8s yet) + Streamlit CRUD for `source_system`/`data_feed`/`model_feed`. Test: create/edit rows via the app.
2. **kind cluster + local storage** — single-node kind with `extraMounts` for `./data-lake`; Postgres moves into-cluster. Test: a debug pod writes a file under `/data-lake/raw/...`, visible on host.
3. **Apache Polaris + Trino, manual MERGE proof** — provision `polaris_db` in the shared Postgres instance; deploy Polaris (Postgres-backed via `relational-jdbc`) + MinIO (S3-compatible storage, `lakehouse` bucket) + Trino; register an `S3`-type Polaris catalog against MinIO (not the originally-planned `FILE` type — see "Polaris + MinIO gotchas" above for why); create a table in `clean`, run a manual `MERGE INTO staging` from Trino. Retires the highest-risk assumption before automating anything.
4. **dbt clean→staging** — `clean` as dbt source, `stg_<feed>` incremental merge model built on the shared `_key_hash`/`_attr_hash` macro (join on key hash, update gated on attr hash actually differing). Add the `trino__current_timestamp()` precision-6 macro override before any snapshot work in Phase 7. Test: idempotent re-runs that produce zero writes when source data is unchanged, not just the same end result.
5. **Dagster wiring (stubbed extraction)** — `dagster dev` locally + `K8sRunLauncher` for op pods; `dagster-dbt` loads the Phase 4 project; stub landing/raw/clean assets feed the real dbt staging asset; wire `data_feed_run`/`data_model_run` writes.
6. **Spark Operator + real raw→clean** — deploy `spark-operator`; replace the stub with a real PySpark job (SparkApplication CR from a Dagster op), including schema validation against `schema_registry`.
7. **Model layer: Type 1/Type 2 dims + facts** — dbt snapshots (`check` strategy, `check_cols: ['_attr_hash']`, `unique_key: '_key_hash'`, `snapshot_meta_column_names`) for `scd_type=2` dimensions; plain incremental `merge` models using the same `_key_hash`/`_attr_hash` pattern for `scd_type=1` dimensions; incremental fact models joined to dimension `_scd_id`, using the same hash pattern where facts are mutable. Add the `int_<feed>_with_deletes.sql` intermediate model for `deletions_enabled` feeds. Test both a Type 1 and a Type 2 dimension, and both an update and a deletion, before moving on.
8. **Serve layer** — codegen step generates `_latest`/`_historical` dbt view models from `model_feed`, wired as a Dagster asset downstream of model.
9. **End-to-end hardening** — one real source (REST API or CSV drop), Dagster schedules/sensors, full watermark handling, failure-path testing and safe re-run.
10. **Metadata data model review** — by this point every phase that earlier columns were speculatively provisioned for (Spark opt-in, incremental extraction, scheduling, schema evolution, real fact/dimension `model_feed` rows) has actually been built. Audit the full metadata schema (`source_system`, `data_feed`, `schema_registry`, `model_feed`, `model_feed_source`, `data_feed_run`, `data_model_run`) against what the code actually reads/writes by now, not what it was provisioned for — the tech-debt audit in Learnings.md ("Tech-debt cleanup pass") already found several columns defined-but-never-read (`processing_engine`, `landing_path_template`/`raw_path_template`, `incremental_column`/`incremental_column_type`, `schedule_cron`, `schema_registry` versioning) that were deliberately left alone at the time specifically because the phases that would use them weren't built yet — this is that checkpoint. For each: either the corresponding code now genuinely reads/writes it (confirm, don't assume), or it's still dead and should be dropped (no migrations in this project — redefine the table directly, see Learnings.md). Also revisit whether `data_model_run.model_key`/`uses_feeds` should now route through real `model_feed` rows instead of free text, now that Phase 7 has given `model_feed` actual fact/dimension rows to reference — the walk-back reasoning in Learnings.md (staging isn't a fact or dimension, doesn't fit `model_feed`'s Kimball-specific columns) still applies to *staging* specifically, but a fact/dim's own `data_model_run` rows arguably should FK to `model_feed` for real now that it's populated.
11. **(Stretch) Azure config swap proof** — two-sided swap against a real ADLS Gen2 account: update Trino's `iceberg.properties` (drop the `hive.s3.*`/MinIO properties for `fs.azure.enabled=true` + `azure.oauth.*`), and re-register the Polaris catalog's storage config from `S3` to `AZURE` (`default-base-location=abfss://...`, tenant/app credentials). Validates the portability claim end-to-end.

## Verification

- Phases 1–2: manual CRUD via Streamlit UI; manual file-visibility check on host filesystem.
- Phase 3: Trino queries against a manually-created Iceberg table (`SELECT`, then `MERGE INTO`) to prove the Trino+Polaris+MinIO stack works before any code depends on it — confirmed working, including a real update-vs-insert `MERGE` (one matched row updated, one new row inserted, others untouched).
- Phases 4, 7, 8: `dbt build` + `dbt test` runs green. Confirm a re-run against unchanged source data produces **zero writes** (no new Iceberg snapshot for untouched rows) — proving the `_attr_hash` gate actually works, not just that the merge is logically idempotent. For a `scd_type=2` dimension: simulate an attribute change and confirm a new version is inserted with `_valid_from`/`_valid_to` set correctly and a new `_scd_id`; simulate a deletion (remove the row from a full-load feed) and confirm `int_<feed>_with_deletes.sql` synthesizes an `is_deleted=true` row that produces a new version, not a special-cased row. For a `scd_type=1` dimension: confirm the same two scenarios update the existing row in place with no new row created, and confirm an unchanged re-run writes nothing at all.
- Phases 5–6: trigger a Dagster asset materialization from the UI/CLI, confirm pods launch in kind (`kubectl get pods`), confirm `data_feed_run`/`data_model_run` rows are written with correct status/row counts.
- Phase 9: full pipeline run against one real source end-to-end, then a deliberate mid-pipeline failure to confirm re-run picks up cleanly from `data_feed_run`/`data_model_run` state.
- Phase 10: for every column flagged in the audit, a concrete before/after — either a real code call site now exists (name it), or the column is gone from the DDL. No column should remain in a "maybe someone reads it" ambiguous state.
