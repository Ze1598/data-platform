# Metadata Store — Data Model

This is the current schema of the platform's metadata store (`metadata/db/init/01_platform_metadata.sql`).

---

## `source_system`

One row per upstream system this platform extracts from (a database, an API, a file-drop source, a SaaS product).

| Column | Type | Constraints |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| code | text | not null, unique |
| name | text | not null |
| description | text | nullable |
| system_type | text | not null, check in `('database','api','file_drop','saas')` |
| connector_kind | text | nullable, check in `('postgres','csv','json_file','rest')` — which `processing/connectors/` implementation extracts from this system. NULL means this system's feeds keep a fully hand-written asset file, not connector/codegen-driven (e.g. `customers`/`sales`' synthetic stub generators). See `scripts/generate_dagster_pipeline.py`. |
| base_location | text | nullable — root for connectivity: a SQL Server name, a storage account container, an API base URL. For `connector_kind='rest'`, this is the connector's `base_url`. |
| connection_user | text | nullable — auth principal for this system |
| connection_secret | text | nullable — **not** the actual secret; a reference/path to where the real credential lives in a vault (e.g. Azure Key Vault) |
| connection_config | jsonb | not null, default `{}` |
| is_active | boolean | not null, default true |
| created_at | timestamptz | not null, default now() |
| updated_at | timestamptz | not null, default now(), trigger-maintained |

**Joins/lookups**: `data_feed.source_system_id` → `source_system.id` (one source system has many feeds).

---

## `data_feed`

One row per source object/table/endpoint to extract — a database table, an API endpoint, a file-drop pattern. User-authored via the frontend CRUD.

| Column | Type | Constraints |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| source_system_id | uuid | not null, FK → `source_system(id)`, indexed |
| friendly_name | text | not null, unique — natural idempotency key for seeding/lookup; what asset code and the seed script key off of |
| source_object_name | text | not null — the object's actual name in the source system (e.g. `"schema.table"` for a database, `"sales.csv"` for a flat file) |
| batch_group | uuid | not null — groups feeds so pipelines can run per-batch, not per-feed. Denormalized (no lookup table) — the same value must be entered consistently across every feed row in that batch. Every feed must belong to a batch: the platform tracks and schedules runs by batch or model schema, never by a bare individual feed — a feed with no natural batch mate is still its own singleton batch |
| batch_group_friendly_name | text | not null — human-readable label for the batch, repeated per row |
| batch_feed_hierarchy | int | not null, default 0 — feeds sharing the same tier can extract in parallel; lower tiers must complete before higher tiers within the same `batch_group` |
| extraction_type | text | not null, check in `('full','incremental')` |
| watermark_column | text | nullable; table constraint `extraction_type = 'full' OR watermark_column IS NOT NULL` |
| extraction_config | jsonb | nullable — arbitrary per-feed JSON for feed-specific extraction parameters (e.g. `metadata_runs`' Postgres connector query, `{"query": "..."}`; a Postgres single-table feed's optional `{"table_name": "..."}` for real primary-key discovery) |
| source_pk | jsonb | not null, default `[]` — array of column names identifying a row in the *source*; extraction-only, not the same concept as any model-layer key |
| processing_engine | text | not null, default `'polars'`, check in `('polars','spark')` |
| pipeline_steps | text | not null, default `'0,1,2'` — comma-separated `pipeline_steps.id` values naming which of the three pipeline steps (extraction/transformation/serving — a different axis from the raw/clean/staging/model/serve *schemas*, see `pipeline_steps` below) this feed's master pipeline actually runs. Resolved live, per run, by `master_pipeline`'s own op, before it decides whether to launch this feed's `EXTRACTION_JOBS[feed]` at all — not baked into codegen |
| last_watermark_value | text | nullable — denormalized current watermark |
| ods_enabled | boolean | not null, default false — when true and this feed owns zero `lakehouse_models` rows, the platform automatically delivers an ODS (Operational Data Store) table: `clean` data pushed as-is (no casts) through an auto-generated `staging` + Type 1 `model` layer, driven purely by `schema_registry`. Silently ignored if any `lakehouse_models` row references this feed (a hand-modeled data model always takes precedence — see "ODS layer" below) |
| batch_ods_name | text | nullable — which ODS "domain" (dbt project, see `dbt/domains/`) this feed's ODS table belongs to, playing the same role `lakehouse_models.model_schema` plays for hand-modeled domains. Only meaningful when `ods_enabled=true`. Defaults to this row's own `batch_group_friendly_name` when a feed first enables ODS (a frontend convenience), but is a real, independently-stored, independently-editable value from that point on — multiple `ods_enabled` feeds sharing the same `batch_ods_name` group into one ODS domain project. Allowed to collide with a real `model_schema` value (that domain would just host both hand-modeled and auto-generated ODS tables); not guarded against. A `batch_group` is expected to map to exactly one `batch_ods_name` (1:1) when triggered via `master_pipeline`'s `orchestration_kind='batch_group'` path — not enforced anywhere today, see `Backlog.md` |
| is_active | boolean | not null, default true |

**Joins/lookups**: `source_system_id` → `source_system.id`. `batch_group` is a bare grouping value (no FK target). `id` is referenced by `schema_registry.data_feed_id`, `lakehouse_models.depends_on_feeds` (comma-separated, not a real FK), `data_processing_runs.data_feed_id`, and `ingestion_triggers.controlling_object_id` (when `controlling_object_type='feed'`).

---

## `schema_registry`

Versioned expected schema of each feed's `clean`-layer output.

| Column | Type | Constraints |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| data_feed_id | uuid | not null, FK → `data_feed(id)` |
| version | int | not null |
| column_definitions | jsonb | not null |
| primary_key_columns | jsonb | not null, default `[]` — the resolved primary key for this feed, precedence: `data_feed.source_pk` (manual metadata entry) wins if non-empty; else a live-discovered key (Postgres catalog introspection only, see `PostgresConnector.discover_primary_key()`); else empty, meaning no key is known at all. Persisted here (not read from `data_feed.source_pk` directly at runtime) so every consumer reads one resolved source of truth. Currently only consumed by the ODS layer, to decide upsert-by-key vs. insert-only |
| is_current | boolean | not null, default true |
| effective_from | timestamptz | not null, default now() |
| effective_to | timestamptz | nullable |
| created_at | timestamptz | not null, default now() |
| created_by | text | nullable |

Constraints: unique `(data_feed_id, version)`; partial unique index on `(data_feed_id) WHERE is_current` — one current version per feed.

**Joins/lookups**: `data_feed_id` → `data_feed.id`.

**Ownership**: exclusively the extraction step's concern (`connectors.schema_registry_sync.sync_schema_registry()`, called from each feed's `extraction_<feed>` asset) — discovery and the registry write both complete before `clean_<feed>` ever runs. `clean_<feed>` only ever reads it (`PostgresMetadataResource.get_current_schema()`), never writes it. Never hand-seeded — a from-scratch feed or a from-scratch platform is expected to have zero rows here until that feed's first real extraction run, not an error state to special-case around. (Corrected 2026-07-16 — `scripts/seed_metadata_db.py` used to hand-seed a row per feed, and REST/JSON connector kinds' generated `clean_<feed>` used to perform discovery itself; both fixed, see `Learnings.md`.)

---

## ODS layer

Resolves the "what happens when a feed never gets a hand-modeled dimension/fact" question — rather than a feed with `pipeline_steps`' transformation step deselected building nothing, `data_feed.ods_enabled` lets the platform deliver a real, fully automatic passthrough table instead. Same principle as skipping the standard serve views: consumers should never be left with nothing to query, whether that's a hand-modeled table's standard view or an ODS table's own.

**Trigger**: `data_feed.ods_enabled = true` **and** zero `lakehouse_models` rows reference this feed (`owning_feed_id`). A hand-modeled data model always takes precedence — the flag is silently ignored if one exists, treated as "forgot to disable it," not an error.

**Primary key precedence**, resolved at schema-sync time and persisted to `schema_registry.primary_key_columns`:
1. `data_feed.source_pk` if non-empty (manual metadata entry — required intentional entry, so it wins over anything discovered).
2. Else a live-discovered key — Postgres catalog introspection (`PostgresConnector.discover_primary_key()`) when the connector was given a single real table to introspect (`data_feed.extraction_config.table_name`); always empty for CSV/REST/JSON-file connector kinds, and for a Postgres feed whose query spans more than one table (no single table to introspect against).
3. Else empty — the ODS table is **insert-only**: a plain incremental append, no `unique_key`, no dedup for `extraction_type='incremental'` feeds (correct because `clean.<feed>` only ever contains new rows since the last watermark). For `extraction_type='full'` feeds with no key, the table is instead fully replaced every run (`materialized='table'`) — `clean.<feed>` re-delivers the complete dataset every run, so an append would duplicate every previously-seen row.

**Mechanics**: fully automatic, zero hand-written SQL, `clean → staging → model`, generated by `scripts/generate_ods_models.py` from `schema_registry` alone — no casts, only the standard technical columns (`_key_hash`/`_attr_hash` at staging, plus `_scd_id`/`_valid_from`/`_valid_to`/`_updated_at` at the model layer). Always Type 1 (upsert-in-place, insert-only, or full-replace — never Type 2). No `is_deleted`/deletion-synthesis concept — no `deletes_enabled`-equivalent exists for `data_feed`. Standard `_latest`/`_historical` serve views are still generated for an ODS table (`scripts/generate_serve_views.py`), same as any `lakehouse_models` row — consumers never touch `model` directly.

---

## `lakehouse_models`

One row per Kimball fact/dimension table the platform builds — **not** staging (staging stays pure naming-convention, no metadata row).

| Column | Type | Constraints |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| friendly_name | text | not null, unique — human-readable display label only, CRUD/UI identity. **Not** what dbt `ref()` resolves against — `table_name` below is the real technical identifier |
| table_name | text | not null, unique — the technical identifier: drives both the physical table alias and the dbt model's own filename, following the `<model_schema>_<fct\|dim>_<name>` convention verbatim (entered as one complete string, not composed from parts — see `scripts/generate_model_scaffolds.py`). This is what makes cross-domain naming collisions structurally impossible: two domains' scaffolded files are never named the same thing, since the domain prefix is baked into the filename itself |
| model_schema | text | not null — which "domain" (dbt project, see `dbt/domains/`) this model belongs to: a business/domain grouping of related lakehouse model tables, not tied to a single source system (a domain's models can depend on feeds from multiple different `source_system` rows). **Not** a physical Trino/Iceberg schema name — the physical `staging`/`model`/`serve` schema names are fixed literals (pipeline-stage boundaries) completely independent of a model's domain; domain identity is expressed via `table_name`'s naming convention and via which dbt project a model's files physically live in, not via a separate physical schema per domain |
| batch_hierarchy | int | not null, default 0 — same tiering concept as `data_feed.batch_feed_hierarchy` |
| table_type | text | not null, check in `('fact','dimension')` |
| business_key_columns | jsonb | not null, default `[]` — the model's own natural key |
| tracked_columns | jsonb | not null, default `[]` — the specific attribute columns hash-compared via `_attr_hash` to detect a Type 2 new-version or Type 1 in-place update |
| scd_type | smallint | not null, default 2, check in `(1,2)` |
| updates_enabled | boolean | not null, default true — also drives whether this model's upstream *staging* source(s) merge on attribute change, see "Staging update-tracking rule" below |
| deletes_enabled | boolean | not null, default false |
| watermark_column | text | nullable |
| load_type | smallint | not null, FK → `load_type(id)` |
| depends_on_feeds | text | nullable — comma-separated `data_feed.id` values that must succeed before this model builds |
| owning_feed_id | uuid | not null, FK → `data_feed(id)` — which single feed's per-feed Dagster job/dbt build actually claims this model's AssetKey. Must be one of `depends_on_feeds` (application-enforced). Required even for a single-feed model, so the meaning is never implicit — see `scripts/generate_dagster_pipeline.py` and `Learnings.md`, "A dbt model tagged with two feed tags gets claimed by two competing `@dbt_assets` defs" |
| pipeline_steps | text | not null, default `'1,2'` — comma-separated `pipeline_steps.id` values. A model has no extraction of its own (that belongs to `depends_on_feeds`), so in practice this only ever meaningfully gates `serving` (transformation is all-or-nothing per domain build, not per model within it — see `dbt_assets.py`'s per-feed cherry-picking for the finer-grained mechanism that actually applies within a domain). Resolved at codegen time by `generate_serve_views.py`, not live per-run: a model with serving deselected simply never gets its `_latest`/`_historical` views generated |
| last_watermark_value | text | nullable |
| last_run_id | uuid | nullable |
| is_active | boolean | not null, default true |

**Joins/lookups**: `depends_on_feeds` holds `data_feed.id` values (comma-separated text, not a real FK). `owning_feed_id` → `data_feed.id` (real FK). `load_type` → `load_type.id`. `id` is referenced by `ingestion_triggers.controlling_object_id` (when `controlling_object_type='model'`) and `data_processing_runs.model_key`.

### Staging update-tracking rule

Staging tables have no `lakehouse_models` row of their own, but their merge behavior still needs a source of truth for "does this feed's staging table need to track attribute updates, or is it insert-only." Rule: for a given `data_feed`, find every `lakehouse_models` row whose `depends_on_feeds` includes that feed's `id`. If **any** of them has `updates_enabled = true`, that feed's staging table tracks updates (merges on `_attr_hash` change). If none do — including the case where **zero** models currently depend on the feed — staging defaults to tracking updates too (the safe default: assume changes matter until a model explicitly says otherwise via `updates_enabled = false`). Only once every dependent model agrees `updates_enabled = false` does staging become insert-only.

---

## `load_type`

Lookup table for `lakehouse_models.load_type`.

| Column | Type | Constraints |
|---|---|---|
| id | smallint | PK |
| label | text | not null |
| description | text | nullable |

Seed rows:

| id | label | description |
|---|---|---|
| 0 | full | Full reload every run |
| 1 | incremental_by_id | Incremental, based on a source ID column |
| 2 | incremental_by_timestamp | Incremental, based on a source timestamp column |
| 3 | incremental_by_custom_query | Incremental, based on a custom query |

**Joins/lookups**: referenced by `lakehouse_models.load_type`.

---

## `pipeline_steps`

Lookup table for `data_feed.pipeline_steps` / `lakehouse_models.pipeline_steps`. **Not the same axis as the `raw`/`clean`/`staging`/`model`/`serve` schemas `data_processing_runs` tracks** — those are storage layers (*where* data lives), these are pipeline steps (*what process phase* is running, and — since Roadmap.md "Master pipeline orchestration" — which of the three independent `EXTRACTION_JOBS`/`MODELING_JOBS`/`SERVING_JOBS` job types actually runs). A single step can span multiple schemas (extraction writes both `raw` and `clean`, bundled into one job — `raw` exists specifically to feed `clean`, so they're never split into separate jobs), so the two are deliberately kept as separate vocabularies rather than collapsed into one.

| Column | Type | Constraints |
|---|---|---|
| id | smallint | PK |
| label | text | not null |
| description | text | nullable |

Seed rows:

| id | label | description |
|---|---|---|
| 0 | extraction | Fetch from the source, land/copy it durably, and validate it into clean — the only step that ever connects to a data source |
| 1 | transformation | Business logic: clean → staging → model |
| 2 | serving | Serve-layer view generation from model |

**Joins/lookups**: referenced by `data_feed.pipeline_steps` and `lakehouse_models.pipeline_steps` (both comma-separated text, not real FKs — same convention as `depends_on_feeds`).

---

## `ingestion_triggers`

Metadata for how a feed/model's `master_pipeline` run actually gets kicked off — a cron schedule, or a storage/sensor trigger watching a feed's own landing directory for a new file. Renamed from the original `schedule` table, which only covered the cron case (see Roadmap.md/Backlog.md for the generalization). A build-time codegen step (`scripts/generate_dagster_pipeline.py`, matching the serve-view generator's pattern) reads this table and constructs real Dagster `ScheduleDefinition`/`SensorDefinition` objects — the definition object itself has to be code, but its cron string (or feed target) and what it controls live here. Every generated trigger targets the same single `master_pipeline` job (Roadmap.md "Master pipeline orchestration") — exactly one Dagster definition per row, no per-feed expansion: a feed-type row resolves that feed's own `batch_group_friendly_name` and fires `master_pipeline` with `orchestration_kind='batch_group'`; a model-type row (schedule-only — see below) resolves that model's own `model_schema` and fires it with `orchestration_kind='model_schema'` directly — `master_pipeline` itself reverse-engineers whichever feeds it actually needs live, from Postgres, rather than the trigger enumerating them. Every generated trigger's execution function re-reads `is_active` live (at each schedule tick, or each sensor evaluation) so disabling a trigger here takes effect without a redeploy, and defaults to `DefaultScheduleStatus.STOPPED`/`DefaultSensorStatus.STOPPED` in Dagster regardless of this column's value — `is_active` controls whether the trigger *exists and fires when turned on*, not Dagster's own manual on/off toggle.

| Column | Type | Constraints |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| trigger_type | text | not null, check in `('schedule','sensor')` |
| cron | text | nullable; table constraint `trigger_type <> 'schedule' or cron is not null` — only meaningful (and only required) for a schedule-type trigger |
| controlling_object_id | uuid | not null — polymorphic: a `data_feed.id` or a `lakehouse_models.id`, depending on `controlling_object_type` |
| controlling_object_type | text | not null, check in `('model','feed')` |
| is_active | boolean | not null, default true — lets a trigger be disabled without deleting the row |

Constraints: unique `(controlling_object_type, controlling_object_id)` — at most one trigger per controlled feed/model (a feed/model picks schedule **or** sensor, never both at once — not an oversight). This is also what makes idempotent seeding possible (`scripts/seed_metadata_db.py`'s `seed_ingestion_trigger()` uses `ON CONFLICT (controlling_object_type, controlling_object_id) DO NOTHING`, the same pattern every other table's natural-key seeding follows). A second check constraint, `trigger_type <> 'sensor' or controlling_object_type = 'feed'`, makes sensor-type feed-only in the DB itself — a model has no source/landing concept of its own to watch. A sensor is additionally only meaningful for a feed whose `source_system.connector_kind` is `csv` or `json_file` (the only kinds with a landing directory) — that half of the eligibility check reaches `source_system` through two joins, so it can only ever be an application-layer check (the frontend CRUD page), not a DB constraint.

**Joins/lookups**: `controlling_object_id` → `data_feed.id` when `controlling_object_type='feed'`, or → `lakehouse_models.id` when `controlling_object_type='model'`. Not a real FK (polymorphic target).

---

## `data_processing_runs`

One row per individual feed-run or model-run per job execution. Spans the entire pipeline, raw through serve, in one wide table.

| Column | Type | Constraints |
|---|---|---|
| run_id | uuid | PK, default `gen_random_uuid()` |
| data_feed_id | uuid | nullable, FK → `data_feed(id)` — populated for a feed-run row |
| model_key | text | nullable — populated for a model-run row (corresponds to `lakehouse_models.friendly_name`, not a real FK) |
| uses_feeds | text | nullable — comma-separated `data_feed.friendly_name` values, populated alongside `model_key` |
| tracking_group | text | not null — either a `batch_group` value or a `model_schema` value, depending on `tracking_group_type` |
| tracking_group_type | text | not null, check in `('batch_group','model_schema')` |
| master_dagster_run_id | text | not null — the master pipeline's own `dagster_run_id` (Roadmap.md "Master pipeline orchestration"), created once by `record_run_started()`/`record_model_run_started()` before any child stage job runs. This row's primary identifying key — not any individual stage's run id |
| storage_watermark | text | nullable — the watermark folder path (`YYYY/MM/DD/HH/MM/SS`) this run's raw data is extracted into, generated once by `record_run_started()` at row-creation time. Populated for feed-run rows only (a model-run row never touches raw). Pins the raw read path unambiguously: `clean` reads from exactly this path (via `PostgresMetadataResource.IngestionStepLog.storage_watermark`), rather than relying on Dagster run-id parity between the raw and clean steps of the same `EXTRACTION_JOBS[feed]` job. Not to be confused with `raw_watermark_value_start`/`end` below (the source's own incremental-pull watermark values — a different concept) |
| extraction_dagster_run_id | text | nullable — `EXTRACTION_JOBS[feed]`'s own `dagster_run_id` (spans the raw+clean schema stages as one job/run — raw exists specifically to feed clean, so they're not split into separate jobs), a genuinely separate Dagster run from the master and from its sibling stages |
| transformation_dagster_run_id | text | nullable — `MODELING_JOBS[domain]`'s own `dagster_run_id` |
| serving_dagster_run_id | text | nullable — `SERVING_JOBS[domain]`'s own `dagster_run_id` |
| job_started_timestamp | timestamptz | not null, default now() |
| job_ended_timestamp | timestamptz | nullable |
| job_successful | boolean | nullable |
| *(×5, prefixed `raw_`, `clean_`, `staging_`, `model_`, `serve_`)* — is_\*_successful, \*_end_timestamp, \*_error_message, \*_rows_read, \*_rows_inserted, \*_rows_updated, \*_rows_deleted, \*_output_path, \*_watermark_value_start, \*_watermark_value_end | mixed | all nullable — same 9-column pattern per stage, one group per stage. No `landing_*` group — "landing" was never a real pipeline concept (see Roadmap.md's terminology cleanup), just a historical mislabeling of the fetch sub-step within extraction; its outcome/watermark tracking is part of `raw_*`'s columns |
| created_at | timestamptz | not null, default now() |

Constraints: partial unique indexes `(data_feed_id, master_dagster_run_id) WHERE data_feed_id IS NOT NULL` and `(model_key, master_dagster_run_id) WHERE model_key IS NOT NULL`; check constraint requiring exactly one of `data_feed_id`/`model_key` to be set.

**Ownership**: the master pipeline (feed-scoped or domain-scoped) creates the row via `record_run_started()`/`record_model_run_started()`, keyed by its own `master_dagster_run_id`. Each of the three independent stage-jobs it subsequently launches (`EXTRACTION_JOBS` — raw+clean as one job/run — `MODELING_JOBS`/`SERVING_JOBS`) is a genuinely separate Dagster run — the master threads its own `master_dagster_run_id` to each as a launch-time run tag, and each stage job looks the row up by `(data_feed_id or model_key, master_dagster_run_id)` (`PostgresMetadataResource._find_run()`), never creating a row itself, and records its own `dagster_run_id` into its own column above. One logical pipeline execution therefore spans up to four distinct Dagster run ids, not one.

**Joins/lookups**: `data_feed_id` → `data_feed.id` (feed-run rows). `model_key` conceptually corresponds to `lakehouse_models.friendly_name` (not a real FK). `tracking_group` corresponds to either `data_feed.batch_group` or `lakehouse_models.model_schema` depending on `tracking_group_type` (not a real FK — polymorphic).
