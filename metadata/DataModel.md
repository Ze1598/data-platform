# Metadata Store — Target Data Model

This is the approved design for the metadata store, superseding
`metadata/db/init/01_platform_metadata.sql`. Implementation is in progress —
see `Progress.md` for status. Every decision below was either an explicit
instruction or a direct answer to a clarifying question; nothing was decided
unilaterally.

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
| **base_location** | text | nullable — root for connectivity: a SQL Server name, a storage account container, an API base URL |
| **connection_user** | text | nullable — auth principal for this system (renamed from the originally-proposed `user`, which is a reserved word in Postgres) |
| **connection_secret** | text | nullable — **not** the actual secret; a reference/path to where the real credential lives in a vault (e.g. Azure Key Vault) |
| connection_config | jsonb | not null, default `{}` — unchanged |
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
| ~~code~~ | — | **removed** — `id` is the identifier, no secondary code |
| **friendly_name** *(renamed from `object_name`)* | text | not null, **unique** — natural idempotency key for seeding/lookup now that `code` is gone; this is what asset code and the seed script key off of |
| ~~name~~ | — | **removed** |
| **source_object_name** | text | not null — the object's actual name in the source system (e.g. `"schema.table"` for a database, `"sales.csv"` for a flat file) |
| **batch_group** | uuid | not null — groups feeds so pipelines can run per-batch, not per-feed. Denormalized (no lookup table) — the same value must be entered consistently across every feed row in that batch. Every feed must belong to a batch: the platform tracks and schedules runs by batch or model schema, never by a bare individual feed — a feed with no natural batch mate is still its own singleton batch |
| **batch_group_friendly_name** | text | not null — human-readable label for the batch, repeated per row |
| **batch_feed_hierarchy** | int | not null, default 0 — feeds sharing the same tier can extract in parallel; lower tiers must complete before higher tiers within the same `batch_group` |
| extraction_type | text | not null, check in `('full','incremental')` — unchanged |
| **watermark_column** *(replaces `incremental_column` + `incremental_column_type`)* | text | nullable; table constraint `extraction_type = 'full' OR watermark_column IS NOT NULL` (same conditional shape as the column it replaces) |
| extraction_config | jsonb | nullable — confirmed unused by any code today; arbitrary per-feed JSON for feed-specific extraction parameters |
| ~~landing_path_template~~ / ~~raw_path_template~~ | — | **removed** — path is a pure code convention: `raw/<batch_group_friendly_name>/<friendly_name>/<extraction_watermark>`, where `extraction_watermark` is a `YYYY/MM/DD/HH/MM/SS` folder, computed at runtime, never stored |
| **source_pk** *(renamed from `business_key_columns`)* | jsonb | not null, default `[]` — array of column names identifying a row in the *source*; extraction-only, not the same concept as any model-layer key |
| ~~staging_table_name~~ | — | **removed** — staging tables follow the standard derived name `<batch_group_friendly_name>__<friendly_name>` (double underscore) |
| ~~schedule_cron~~ | — | **removed** — moved to the new `schedule` table below |
| processing_engine | text | not null, default `'polars'`, check in `('polars','spark')` — unchanged |
| ~~updates_enabled~~ | — | **removed** — feeds always fully reload `raw`/`clean`. Staging still merges (accumulates), but its update-tracking behavior is now sourced from `lakehouse_models`, not stored here — see "Staging update-tracking rule" below |
| last_watermark_value | text | nullable — unchanged, denormalized current watermark |
| ~~last_run_id~~ | — | **removed** — derivable from `data_processing_runs` (most recent row for this `data_feed_id`) |
| is_active | boolean | not null, default true — unchanged |
| ~~created_at~~ / ~~updated_at~~ | — | **removed** — user-authored table via CRUD, not processing-driven |

**Joins/lookups**: `source_system_id` → `source_system.id`. `batch_group` is a bare grouping value (no FK target). `id` is referenced by `schema_registry.data_feed_id`, `lakehouse_models.depends_on_feeds` (comma-separated, not a real FK), `data_processing_runs.data_feed_id`, and `schedule.controlling_object_id` (when `controlling_object_type='feed'`).

---

## `schema_registry`

Versioned expected schema of each feed's `clean`-layer output — unchanged from the original design.

| Column | Type | Constraints |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| data_feed_id | uuid | not null, FK → `data_feed(id)` |
| version | int | not null |
| column_definitions | jsonb | not null |
| is_current | boolean | not null, default true |
| effective_from | timestamptz | not null, default now() |
| effective_to | timestamptz | nullable |
| created_at | timestamptz | not null, default now() |
| created_by | text | nullable |

Constraints: unique `(data_feed_id, version)`; partial unique index on `(data_feed_id) WHERE is_current` — one current version per feed.

**Joins/lookups**: `data_feed_id` → `data_feed.id`.

---

## `lakehouse_models` *(renamed from `model_feed`)*

One row per Kimball fact/dimension table the platform builds — **not** staging (staging stays pure naming-convention, no metadata row).

| Column | Type | Constraints |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| **friendly_name** *(renamed from `code`)* | text | not null, unique — this is what dbt `ref()` resolves against, so uniqueness is load-bearing |
| **model_schema** | text | not null — which Trino/Iceberg schema this table lands in (`model`, etc.) — the existing namespace concept, explicit per-row |
| **batch_hierarchy** | int | not null, default 0 — same tiering concept as `data_feed.batch_feed_hierarchy` |
| **table_type** *(renamed from `model_type`)* | text | not null, check in `('fact','dimension')` — unchanged scope, staging excluded |
| ~~staging_source_data_feed_id~~ | — | **removed** — superseded by `depends_on_feeds` below |
| business_key_columns | jsonb | not null, default `[]` — unchanged; the model's own natural key |
| tracked_columns | jsonb | not null, default `[]` — unchanged; the specific attribute columns hash-compared via `_attr_hash` to detect a Type 2 new-version or Type 1 in-place update |
| ~~surrogate_key_column~~ | — | **removed** — the SK column name is a fixed platform standard |
| scd_type | smallint | not null, default 2, check in `(1,2)` — unchanged |
| updates_enabled | boolean | not null, default true — unchanged column, **expanded scope**: now also drives whether this model's upstream *staging* source(s) merge on attribute change — see below |
| **deletes_enabled** *(renamed from `deletions_enabled`)* | boolean | not null, default false |
| watermark_column | text | nullable — unchanged, already existed here |
| **load_type** | smallint | not null, FK → `load_type(id)` (new lookup table below) |
| **depends_on_feeds** | text | nullable — comma-separated `data_feed.id` values that must succeed before this model builds; replaces both `staging_source_data_feed_id` and the deleted `model_feed_source` bridge table |
| last_watermark_value | text | nullable — unchanged |
| last_run_id | uuid | nullable — unchanged |
| is_active | boolean | not null, default true — unchanged |
| ~~created_at~~ / ~~updated_at~~ | — | **removed** — same reasoning as `data_feed`: user-authored via CRUD, not processing-driven |

**Joins/lookups**: `depends_on_feeds` holds `data_feed.id` values (comma-separated text, not a real FK). `load_type` → `load_type.id`. `id` is referenced by `schedule.controlling_object_id` (when `controlling_object_type='model'`) and `data_processing_runs.model_key`.

### Staging update-tracking rule

Staging tables have no `lakehouse_models` row of their own, but their merge behavior still needs a source of truth for "does this feed's staging table need to track attribute updates, or is it insert-only." Rule: for a given `data_feed`, find every `lakehouse_models` row whose `depends_on_feeds` includes that feed's `id`. If **any** of them has `updates_enabled = true`, that feed's staging table tracks updates (merges on `_attr_hash` change). If none do — including the case where **zero** models currently depend on the feed — staging defaults to tracking updates too (the safe default: assume changes matter until a model explicitly says otherwise via `updates_enabled = false`). Only once every dependent model agrees `updates_enabled = false` does staging become insert-only.

**Known consequence**: `sales` has two dependent models (`dim_branch`, `fct_sales`), both defaulting to `updates_enabled = true` — so `sales`'s staging table goes back to full update-tracking under this rule, reversing the feed-level insert-only setting made earlier this session. That earlier setting no longer has anywhere to live (`data_feed.updates_enabled` is gone) — if `sales` should stay insert-only, that now has to be expressed by setting `updates_enabled = false` on both `dim_branch` and `fct_sales`.

---

## `model_feed_source` — **deleted**

Its entire purpose (tracking which feed(s) a multi-source fact draws from) is now covered by `lakehouse_models.depends_on_feeds`.

---

## `load_type` *(new)*

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

## `schedule` *(new)*

Metadata for Dagster schedules. A build-time codegen step (matching the existing serve-view generator's pattern) would read this table and construct the real Dagster `ScheduleDefinition` Python objects — the schedule object itself has to be code, but its cron string and what it controls can live here. **Scope note**: this implementation pass creates the table and makes it seedable/CRUD-manageable; it does not yet build the codegen step that turns rows into live Dagster schedules (`police_crimes`'s schedule stays on its existing hardcoded-cron mechanism for now) — that's follow-on work, not part of the metadata schema itself.

| Column | Type | Constraints |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| cron | text | not null |
| controlling_object_id | uuid | not null — polymorphic: a `data_feed.id` or a `lakehouse_models.id`, depending on `controlling_object_type` |
| controlling_object_type | text | not null, check in `('model','feed')` |
| is_active | boolean | not null, default true — lets a schedule be disabled without deleting the row |

**Joins/lookups**: `controlling_object_id` → `data_feed.id` when `controlling_object_type='feed'`, or → `lakehouse_models.id` when `controlling_object_type='model'`. Not a real FK (polymorphic target).

---

## `data_processing_runs` *(renamed + merged from `data_feed_run` + `data_model_run`)*

One row per individual feed-run or model-run per job execution — same grain as before the merge. Spans the entire pipeline, landing through serve, in one wide table instead of two.

| Column | Type | Constraints |
|---|---|---|
| run_id | uuid | PK, default `gen_random_uuid()` |
| data_feed_id | uuid | nullable, FK → `data_feed(id)` — populated for a feed-run row |
| model_key | text | nullable — populated for a model-run row (corresponds to `lakehouse_models.friendly_name`, not a real FK) |
| uses_feeds | text | nullable — comma-separated `data_feed.friendly_name` values, populated alongside `model_key` |
| **tracking_group** | text | not null — either a `batch_group` value or a `model_schema` value, depending on `tracking_group_type` |
| **tracking_group_type** | text | not null, check in `('batch_group','model_schema')` |
| dagster_run_id | text | not null |
| job_started_timestamp | timestamptz | not null, default now() |
| job_ended_timestamp | timestamptz | nullable |
| job_successful | boolean | nullable |
| *(×6, prefixed `landing_`, `raw_`, `clean_`, `staging_`, `model_`, `serve_`)* — is_\*_successful, \*_end_timestamp, \*_error_message, \*_rows_read, \*_rows_inserted, \*_rows_updated, \*_rows_deleted, \*_output_path, \*_watermark_value_start, \*_watermark_value_end | mixed | all nullable — same 9-column pattern per stage, now six stage groups instead of three |
| created_at | timestamptz | not null, default now() |

Constraints: partial unique indexes `(data_feed_id, dagster_run_id) WHERE data_feed_id IS NOT NULL` and `(model_key, dagster_run_id) WHERE model_key IS NOT NULL` — preserves the original per-table uniqueness guarantee now that both row kinds share one table; check constraint requiring exactly one of `data_feed_id`/`model_key` to be set.

**Joins/lookups**: `data_feed_id` → `data_feed.id` (feed-run rows). `model_key` conceptually corresponds to `lakehouse_models.friendly_name` (not a real FK). `tracking_group` corresponds to either `data_feed.batch_group` or `lakehouse_models.model_schema` depending on `tracking_group_type` (not a real FK — polymorphic).
