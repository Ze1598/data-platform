# Metadata Store — Target Data Model

This is a **design document**, not the current implementation. It reflects the
user's review of the original schema (`metadata/db/init/01_platform_metadata.sql`)
plus every clarification gathered in response — see the conversation this was
produced in for the full back-and-forth. **Nothing in this file has been built
yet.** DDL/code changes are a separate, explicitly-approved step.

Marked `PROPOSED` where a detail wasn't specified and a reasonable default was
picked for review — everything else reflects an explicit instruction or a
direct answer to a clarifying question.

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
| **base_location** | text | nullable — root for connectivity: a SQL Server name, a storage account container, an API base URL. Inserted directly after `system_type` per instruction. |
| **user** | text | nullable — auth principal for this system. ⚠️ `user` is a reserved word in Postgres (aliases `CURRENT_USER` in some contexts); every reference to it needs quoting (`"user"`). Workable, but flag if you'd rather use `connection_user` to avoid the friction. |
| **secret** | text | nullable — **not** the actual secret; a reference/path to where the real credential lives in a vault (e.g. Azure Key Vault). |
| connection_config | jsonb | not null, default `{}` — unchanged. Possible overlap with the three new columns above worth a glance once this is real (`connection_config` may end up holding things `base_location`/`user`/`secret` now cover). |
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
| **friendly_name** *(renamed from `object_name`)* | text | not null — user-readable label |
| ~~name~~ | — | **removed** |
| **source_object_name** | text | not null — the object's actual name in the source system (e.g. `"schema.table"` for a database, `"sales.csv"` for a flat file) |
| **batch_group** | uuid | nullable — groups feeds so pipelines can run per-batch, not per-feed. `PROPOSED` denormalized (no lookup table, per your answer) — same batch_group value must be entered consistently across every feed row in that batch by whoever's authoring them. |
| **batch_group_friendly_name** | text | nullable — human-readable label for the batch, repeated per row |
| **batch_feed_hierarchy** | int | `PROPOSED` not null, default 0 — feeds sharing the same tier can extract in parallel; lower tiers must complete before higher tiers within the same `batch_group` |
| extraction_type | text | not null, check in `('full','incremental')` — unchanged |
| **watermark_column** *(replaces `incremental_column` + `incremental_column_type`)* | text | nullable; table constraint `extraction_type = 'full' OR watermark_column IS NOT NULL` (same conditional shape as the column it replaces) |
| ~~incremental_column~~ / ~~incremental_column_type~~ | — | **removed**, consolidated into `watermark_column` |
| extraction_config | jsonb | **nullable now** (was not null default `{}`) — confirmed unused by any code today; arbitrary per-feed JSON for feed-specific extraction parameters, genuinely optional |
| ~~landing_path_template~~ | — | **removed** — landing path is not stored in metadata |
| ~~raw_path_template~~ | — | **removed** — raw path is a pure code convention: `raw/<batch_group>/<friendly_name>/<extraction_watermark>`, where `extraction_watermark` is a `YYYY/MM/DD/HH/MM/SS` folder, computed at runtime, never stored |
| **source_pk** *(renamed from `business_key_columns`)* | jsonb | not null, default `[]` — array of column names identifying a row in the *source*; extraction-only, not the same concept as any model-layer key |
| ~~staging_table_name~~ | — | **removed** — staging tables follow the standard derived name `<batch_group>__<friendly_name>` (double underscore) |
| ~~schedule_cron~~ | — | **removed** — moved to the new `schedule` table below |
| processing_engine | text | not null, default `'polars'`, check in `('polars','spark')` — unchanged |
| ~~updates_enabled~~ | — | **removed** — feeds always fully reload `raw`/`clean`; update/delete tracking is exclusively a `lakehouse_models` concern (staging's merge behavior is now driven by `lakehouse_models.updates_enabled`, not anything on `data_feed`) |
| last_watermark_value | text | nullable — unchanged, denormalized current watermark |
| ~~last_run_id~~ | — | **removed** — derivable from `data_processing_runs` (most recent successful row for this `data_feed_id`) |
| is_active | boolean | not null, default true — unchanged |
| ~~created_at~~ / ~~updated_at~~ | — | **removed** — user-authored table via CRUD, not processing-driven, so standard audit timestamps aren't tracked here |

**Joins/lookups**: `source_system_id` → `source_system.id`. `batch_group` is a bare grouping value (no FK target — no separate batch_group table, per your answer). `id` is referenced by `schema_registry.data_feed_id`, `lakehouse_models.depends_on_feeds` (comma-separated, not a real FK), `data_processing_runs.data_feed_id`, and `schedule.controlling_object_id` (when `controlling_object_type='feed'`).

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

One row per Kimball fact/dimension table the platform builds — **not** staging (staging stays pure naming-convention, no metadata row, per your answer).

| Column | Type | Constraints |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| **friendly_name** *(renamed from `code`)* | text | not null, unique — this is what dbt `ref()` resolves against (matches the existing serve-view codegen pattern), so uniqueness is load-bearing, not cosmetic |
| **model_schema** | text | `PROPOSED` not null — which Trino/Iceberg schema this table lands in (`model`, etc.) — the existing namespace concept, made explicit per-row rather than implied by dbt folder structure |
| **batch_hierarchy** | int | `PROPOSED` not null, default 0 — same tiering concept as `data_feed.batch_feed_hierarchy`, for enforcing some dims/facts to build before others |
| **table_type** *(renamed from `model_type`)* | text | not null, check in `('fact','dimension')` — unchanged scope, staging excluded |
| ~~staging_source_data_feed_id~~ | — | **removed** — superseded by `depends_on_feeds` below |
| business_key_columns | jsonb | not null, default `[]` — unchanged; the model's own natural key (can differ from any single feed's `source_pk` — e.g. a conformed dimension built from a fact source) |
| tracked_columns | jsonb | not null, default `[]` — unchanged; the specific attribute columns hash-compared via `_attr_hash` to detect a Type 2 new-version or Type 1 in-place update, distinct from `business_key_columns` |
| ~~surrogate_key_column~~ | — | **removed** — the SK column name is a fixed platform standard (not configurable per row) |
| scd_type | smallint | not null, default 2, check in `(1,2)` — unchanged |
| updates_enabled | boolean | not null, default true — unchanged column, **expanded scope**: now also governs whether this model's *staging* source merges on attribute change (see `data_feed` above) |
| **deletes_enabled** *(renamed from `deletions_enabled`)* | boolean | not null, default false |
| watermark_column | text | nullable — unchanged, already existed here |
| **load_type** | smallint | `PROPOSED` not null, FK → `load_type(id)` (new lookup table below) |
| **depends_on_feeds** | text | `PROPOSED` nullable — comma-separated `data_feed.id` values that must succeed before this model builds; replaces both `staging_source_data_feed_id` and the deleted `model_feed_source` bridge table. Manual comma-separated entry today; a proper multi-select UI is a deferred front-end decision, not part of this schema. |
| last_watermark_value | text | nullable — unchanged |
| last_run_id | uuid | nullable — unchanged (not mentioned for removal here, unlike `data_feed.last_run_id`) |
| is_active | boolean | not null, default true — unchanged |
| created_at | timestamptz | not null, default now() — unchanged |
| updated_at | timestamptz | not null, default now(), trigger-maintained — unchanged |

⚠️ **Flagging, not deciding**: `data_feed` dropped `created_at`/`updated_at` with the reasoning "user-led table, not processing-driven." `lakehouse_models` is equally user-led/CRUD-managed, but you only stated that reasoning under `data_feed`, so I've left these columns in place here per "anything not mentioned doesn't change." Worth a deliberate yes/no rather than an inconsistency by omission.

**Joins/lookups**: `depends_on_feeds` holds `data_feed.id` values (comma-separated text, not a real FK — same non-enforced pattern as `data_model_run.uses_feeds` today). `load_type` → `load_type.id`. `id` is referenced by `schedule.controlling_object_id` (when `controlling_object_type='model'`) and `data_processing_runs.model_key`/`uses_feeds`.

---

## `model_feed_source` — **deleted**

Its entire purpose (tracking which feed(s) a multi-source fact draws from) is now covered by `lakehouse_models.depends_on_feeds`.

---

## `load_type` *(new)*

`PROPOSED` structure — a lookup table for `lakehouse_models.load_type`, seeded with your four defined values.

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

## `schedule` *(new, name PROPOSED — pick your own if you'd rather)*

Metadata for Dagster schedules. A build-time codegen step (matching the existing serve-view generator's pattern) reads this table and constructs the real Dagster `ScheduleDefinition` Python objects — the schedule object itself has to be code, but its cron string and what it controls can live here.

| Column | Type | Constraints |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| cron | text | not null |
| controlling_object_id | uuid | not null — polymorphic: a `data_feed.id` or a `lakehouse_models.id`, depending on `controlling_object_type` |
| controlling_object_type | text | not null, check in `('model','feed')` |
| `PROPOSED` is_active | boolean | not null, default true — lets a schedule be disabled without deleting the row |

**Joins/lookups**: `controlling_object_id` → `data_feed.id` when `controlling_object_type='feed'`, or → `lakehouse_models.id` when `controlling_object_type='model'`. Not a real FK (polymorphic target), same pattern as `depends_on_feeds`/`uses_feeds` elsewhere in this model.

---

## `data_processing_runs` *(renamed + merged from `data_feed_run` + `data_model_run`)*

One row per individual feed-run or model-run per job execution — **same grain as today** (confirmed: not one row per whole batch). Spans the entire pipeline, landing through serve, in one wide table instead of two.

| Column | Type | Constraints |
|---|---|---|
| run_id | uuid | PK, default `gen_random_uuid()` |
| data_feed_id | uuid | `PROPOSED` nullable, FK → `data_feed(id)` — populated for a feed-run row |
| model_key | text | `PROPOSED` nullable — populated for a model-run row (was not null in the old `data_model_run`; now nullable since a row can be either kind) |
| uses_feeds | text | `PROPOSED` nullable — comma-separated `data_feed.code` values, populated alongside `model_key` |
| **tracking_group** | text | not null — either a `batch_group` value or a `model_schema` value, depending on `tracking_group_type` |
| **tracking_group_type** | text | not null, check in `('batch_group','model_schema')` |
| dagster_run_id | text | not null |
| job_started_timestamp | timestamptz | not null, default now() |
| job_ended_timestamp | timestamptz | nullable |
| job_successful | boolean | nullable |
| *(×6, prefixed `landing_`, `raw_`, `clean_`, `staging_`, `model_`, `serve_`)* — is_\*_successful, \*_end_timestamp, \*_error_message, \*_rows_read, \*_rows_inserted, \*_rows_updated, \*_rows_deleted, \*_output_path, \*_watermark_value_start, \*_watermark_value_end | mixed | all nullable — same 9-column pattern per stage as today, now six stage groups instead of three |
| created_at | timestamptz | not null, default now() |

`PROPOSED` constraints: partial unique index `(data_feed_id, dagster_run_id) WHERE data_feed_id IS NOT NULL` and `(model_key, dagster_run_id) WHERE model_key IS NOT NULL` — preserves the original per-table uniqueness guarantee now that both row kinds share one table. A check constraint requiring exactly one of `data_feed_id`/`model_key` to be set is worth considering too.

**Joins/lookups**: `data_feed_id` → `data_feed.id` (feed-run rows). `model_key` conceptually corresponds to `lakehouse_models.friendly_name` (not a real FK, same free-text pattern as today). `tracking_group` corresponds to either `data_feed.batch_group` or `lakehouse_models.model_schema` depending on `tracking_group_type` (not a real FK — polymorphic).

---

## Open items for your review

Everything marked `PROPOSED` above, plus:
1. `source_system."user"` — reserved-word naming friction; keep as `user` (quoted everywhere) or rename to `connection_user`?
2. `lakehouse_models` keeping `created_at`/`updated_at` while `data_feed` drops them — intentional inconsistency, or should both tables match?
3. `schedule` as a table name — fine, or would you rather something else?
4. `source_system.connection_config` (jsonb, unchanged) now sits alongside the new `base_location`/`user`/`secret` columns — possible overlap worth a look once this is real, not blocking the document.
