-- Platform metadata schema: source_system, data_feed, schema_registry,
-- lakehouse_models, load_type, ingestion_triggers, data_processing_runs.
-- See metadata/DataModel.md for the full design rationale and column-by-
-- column reasoning behind this schema.

create or replace function set_updated_at()
returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

-- ---------------------------------------------------------------------------
-- source_system
-- ---------------------------------------------------------------------------
create table source_system (
    id                 uuid primary key default gen_random_uuid(),
    code               text not null unique,
    name               text not null,
    description        text,
    system_type        text not null check (system_type in ('database', 'api', 'file_drop', 'saas')),
    -- which connector implementation (processing/connectors/) extracts from
    -- this system: 'postgres'/'csv' are tabular (extraction and validation
    -- stay two separate stages); 'rest'/'json_file' are nested-JSON sources
    -- where flattening+discovery+validation combine into one stage, since
    -- flattening is inseparable from establishing the real (flat) schema
    -- contract for this source shape -- see Learnings.md and the connector
    -- library plan for the full reasoning. NULL means this system's feeds
    -- keep a fully hand-written asset file, not connector/codegen-driven
    -- (e.g. customers/sales' synthetic in-memory stub generators).
    connector_kind     text check (connector_kind in ('postgres', 'csv', 'json_file', 'rest')),
    -- root for connectivity: a SQL Server name, a storage account container, an API base URL
    base_location      text,
    -- auth principal for this system
    connection_user    text,
    -- NOT the actual secret -- a reference/path to where the real credential
    -- lives in a vault (e.g. Azure Key Vault)
    connection_secret  text,
    connection_config  jsonb not null default '{}'::jsonb,
    is_active          boolean not null default true,
    created_at         timestamptz not null default now(),
    updated_at         timestamptz not null default now()
);

create trigger trg_source_system_updated_at
    before update on source_system
    for each row execute function set_updated_at();

-- ---------------------------------------------------------------------------
-- data_feed (one row per source object/table/endpoint to extract)
-- ---------------------------------------------------------------------------
create table data_feed (
    id                        uuid primary key default gen_random_uuid(),
    source_system_id          uuid not null references source_system(id),
    -- identity: no separate code, friendly_name is the natural key used for
    -- idempotent seeding, dbt/asset lookups, and CRUD selection
    friendly_name             text not null unique,
    -- the object's actual name in the source system (e.g. "schema.table" for
    -- a database, "sales.csv" for a flat file)
    source_object_name        text not null,
    -- groups feeds so pipelines can run per-batch, not per-feed. Denormalized
    -- (no lookup table) -- the same value must be entered consistently
    -- across every feed row in that batch. Every feed must belong to a
    -- batch (not null) -- the platform tracks and schedules runs by batch
    -- or model schema, never by a bare individual feed; a feed with no
    -- natural batch mate is still its own singleton batch.
    batch_group               uuid not null,
    batch_group_friendly_name text not null,
    -- feeds sharing the same tier can extract in parallel; lower tiers must
    -- complete before higher tiers within the same batch_group
    batch_feed_hierarchy      int not null default 0,
    extraction_type           text not null check (extraction_type in ('full', 'incremental')),
    watermark_column          text,
    -- arbitrary per-feed JSON for feed-specific extraction parameters (unused today)
    extraction_config         jsonb,
    -- column names identifying a row in the source; extraction-only, not the
    -- same concept as a model-layer business key
    source_pk                 jsonb not null default '[]'::jsonb,
    -- which engine runs this feed's raw->clean transform: 'polars' by
    -- default (runs inline in the Dagster op, no extra cluster
    -- infrastructure), 'spark' opt-in for feeds whose volume actually
    -- needs distributed execution (see Learnings.md, Phase 6)
    processing_engine         text not null default 'polars' check (processing_engine in ('polars', 'spark')),
    -- comma-separated pipeline_steps.id values -- which of the three
    -- pipeline steps (extraction/transformation/serving -- extraction
    -- covers raw+clean as one job, see pipeline_steps below) this feed's
    -- master pipeline actually runs, decided live by the master pipeline
    -- (which stage-jobs it launches at all), not baked into codegen --
    -- changing this takes effect on the next run, no regen needed. All
    -- three by default (today's existing full-chain behavior).
    pipeline_steps            text not null default '0,1,2',
    -- denormalized watermark state for the orchestrator; data_processing_runs is the full run history
    last_watermark_value      text,
    is_active                 boolean not null default true,
    -- when true and this feed owns zero lakehouse_models rows, the platform
    -- automatically delivers an ODS (Operational Data Store) table: clean
    -- data pushed as-is (no casts, no transformations) through an
    -- auto-generated staging + Type 1 model layer, driven purely by
    -- schema_registry -- see scripts/generate_ods_models.py. Silently
    -- ignored if any lakehouse_models row references this feed (treated as
    -- "forgot to disable the flag", not an error) -- a hand-modeled
    -- data model always takes precedence.
    ods_enabled               boolean not null default false,
    -- which ODS "domain" (dbt project, see dbt/domains/) this feed's ODS
    -- table belongs to -- each batch group producing ODS output is its own
    -- legitimate individual ODS lakehouse model, same role
    -- lakehouse_models.model_schema plays for hand-modeled domains. Only
    -- meaningful when ods_enabled=true. Defaults to this row's own
    -- batch_group_friendly_name when a feed first enables ODS (frontend
    -- convenience), but is a real, independently-stored, independently
    -- editable value from that point on, not a live derivation -- multiple
    -- ods_enabled feeds sharing the same batch_ods_name group into one ODS
    -- domain project. Allowed to collide with a real model_schema value
    -- (both would just mean that domain hosts hand-modeled and
    -- auto-generated ODS tables together); not guarded against.
    batch_ods_name            text,
    constraint chk_data_feed_watermark_column check (
        extraction_type = 'full' or watermark_column is not null
    )
);

create index idx_data_feed_source_system on data_feed (source_system_id);

-- ---------------------------------------------------------------------------
-- streaming_source (Roadmap Phase 11 generalization, 2026-07-18) -- one row
-- per real-time Kafka->Flink->Iceberg ingestion pipeline. Deliberately a
-- new, standalone top-level concept, NOT a data_feed row and NOT FK'd to
-- source_system -- source_system.connector_kind's vocabulary (postgres/csv/
-- json_file/rest) is pull-based/batch-shaped and doesn't fit a continuous
-- push source; data_feed's extraction_type/watermark_column/
-- last_watermark_value assume a discrete, bounded run, which streaming
-- isn't. Forcing this into either existing table would repeat the exact
-- mistake this project already walked back once (Phase 6: "merging
-- data_model_run into model_feed... would be worse debt than the
-- duplication it was meant to fix").
-- ---------------------------------------------------------------------------
create table streaming_source (
    id                       uuid primary key default gen_random_uuid(),
    friendly_name            text not null unique,
    -- the Kafka topic to consume -- must already exist with real messages
    -- flowing before schema discovery (the frontend's "Discover Schema"
    -- action) can run; this platform has exactly one shared Kafka broker
    -- (kafka.streaming.svc.cluster.local:9092), so bootstrap servers are a
    -- platform-wide constant, not per-row metadata, same category as
    -- Trino/Postgres connection info never being per-feed metadata.
    topic_name               text not null,
    -- target Iceberg table identifier (streaming.<table_name>) -- one
    -- complete string, same "<domain>_<name>"-style convention as
    -- lakehouse_models.table_name, not composed from parts.
    table_name               text not null unique,
    -- which dbt domain (dbt/domains/<model_schema>/) the generated serve
    -- scaffold lands in -- same role as lakehouse_models.model_schema.
    model_schema             text not null,
    -- which discovered column (see schema_registry, controlling_object_type
    -- ='streaming_source') represents event time -- null until schema
    -- discovery has run and the user has picked one; required before
    -- generate_streaming_ingestion.py will generate this source's sink SQL
    -- (the Iceberg sink's INSERT needs a CAST(... AS TIMESTAMP(6)) target).
    event_timestamp_column   text,
    -- Optional per-source Flink resource sizing -- null means "use the
    -- platform default" (see streaming/flink/module.just). Real production
    -- Kubernetes clusters run multi-node with autoscaling, so a JobManager+
    -- TaskManager pair per source (Application Mode -- see Learnings.md/
    -- Roadmap.md for why this, not a shared Session-mode cluster) is
    -- trivial overhead there; these fields exist so a source with real
    -- throughput can be sized deliberately rather than only ever getting
    -- the demo-sized default.
    jobmanager_memory        text,
    taskmanager_memory       text,
    taskmanager_cpu          numeric,
    parallelism              int,
    -- the Flink Kubernetes Operator's own built-in autoscaler
    -- (job.autoscaler.enabled -- adjusts per-job-vertex parallelism from
    -- observed load). Opt-in only -- flagged experimental in the
    -- operator's own current docs, same posture as data_feed.
    -- processing_engine's Polars-default/Spark-opt-in split.
    autoscaler_enabled       boolean not null default false,
    is_active                boolean not null default true
);

-- ---------------------------------------------------------------------------
-- schema_registry (versioned expected schema of each feed/streaming_source's
-- data). Polymorphic (controlling_object_id/controlling_object_type), same
-- pattern as ingestion_triggers below -- generalized 2026-07-18 to also
-- cover streaming_source (Roadmap Phase 11 generalization) alongside the
-- original data_feed, one source of truth for "what does this thing's data
-- look like" across both batch and streaming rather than two parallel
-- concepts. No FK enforced across the polymorphic pair (same reasoning as
-- ingestion_triggers -- a single column can't FK to two different tables).
-- ---------------------------------------------------------------------------
create table schema_registry (
    id                       uuid primary key default gen_random_uuid(),
    controlling_object_id    uuid not null,
    controlling_object_type  text not null check (controlling_object_type in ('feed', 'streaming_source')),
    version                  int not null,
    column_definitions       jsonb not null,
    -- resolved primary key for this feed, precedence: data_feed.source_pk
    -- (manual metadata entry) wins if non-empty; else a live-discovered key
    -- (see connectors.postgres.PostgresConnector.discover_primary_key());
    -- else empty, meaning no key is known at all. Persisted here (not read
    -- from data_feed.source_pk directly at runtime) so every consumer reads
    -- one resolved source of truth. Currently only consumed by the ODS
    -- layer (scripts/generate_ods_models.py) to decide upsert-by-key vs.
    -- insert-only. Not meaningful for controlling_object_type='streaming_source'
    -- (a stream has no primary-key concept the way a batch source does) --
    -- left at its default empty array for streaming rows.
    primary_key_columns      jsonb not null default '[]'::jsonb,
    is_current               boolean not null default true,
    effective_from           timestamptz not null default now(),
    effective_to             timestamptz,
    created_at               timestamptz not null default now(),
    created_by               text,
    unique (controlling_object_id, controlling_object_type, version)
);

-- only one current schema version per feed/streaming_source
create unique index uq_schema_registry_current
    on schema_registry (controlling_object_id, controlling_object_type)
    where is_current;

-- ---------------------------------------------------------------------------
-- load_type (lookup for lakehouse_models.load_type)
-- ---------------------------------------------------------------------------
create table load_type (
    id            smallint primary key,
    label         text not null,
    description   text
);

insert into load_type (id, label, description) values
    (0, 'full', 'Full reload every run'),
    (1, 'incremental_by_id', 'Incremental, based on a source ID column'),
    (2, 'incremental_by_timestamp', 'Incremental, based on a source timestamp column'),
    (3, 'incremental_by_custom_query', 'Incremental, based on a custom query');

-- ---------------------------------------------------------------------------
-- pipeline_steps (lookup for data_feed.pipeline_steps / lakehouse_models.
-- pipeline_steps). NOT the same axis as the raw/clean/staging/model/serve
-- schemas data_processing_runs tracks -- those are storage layers (where
-- data lives), these are pipeline steps (what process phase is running,
-- and which independent Dagster job runs it -- see Roadmap.md "Master
-- pipeline orchestration"). A single step can span multiple schemas
-- (extraction covers both raw and clean, in one job/run -- raw exists
-- specifically to feed clean, so the two aren't split into separate
-- steps), so the two axes are deliberately kept separate rather than
-- collapsed into one vocabulary.
-- ---------------------------------------------------------------------------
create table pipeline_steps (
    id            smallint primary key,
    label         text not null,
    description   text
);

insert into pipeline_steps (id, label, description) values
    (0, 'extraction', 'Fetch from the source, land/copy it durably, and validate it into clean -- the only step that ever connects to a data source'),
    (1, 'transformation', 'Business logic: clean -> staging -> model'),
    (2, 'serving', 'Serve-layer view generation from model');

-- ---------------------------------------------------------------------------
-- lakehouse_models (fact/dim config -- NOT staging; staging stays pure
-- naming-convention with no metadata row of its own)
-- ---------------------------------------------------------------------------
create table lakehouse_models (
    id                    uuid primary key default gen_random_uuid(),
    -- human-readable display label only -- CRUD/UI identity, not a
    -- technical identifier. table_name below is what dbt ref()/the
    -- physical alias/the dbt model filename actually key off.
    friendly_name         text not null unique,
    -- the technical identifier: drives both the physical table alias and
    -- the dbt model's own filename, following the
    -- "<model_schema>_<fct|dim>_<name>" convention verbatim (entered as a
    -- complete string, not composed from parts) -- see
    -- scripts/generate_model_scaffolds.py. This is what makes
    -- cross-domain naming collisions structurally impossible: two
    -- domains' scaffolded files are never named the same thing, since the
    -- domain prefix is baked into the filename itself, not just the alias.
    table_name            text not null unique,
    -- which "domain" (dbt project, see dbt/domains/) this model belongs
    -- to -- a business/domain grouping of related lakehouse model tables,
    -- not tied to a single source system (a domain's models can depend on
    -- feeds from multiple different source_system rows). Physical
    -- staging/model/serve Trino/Iceberg schema NAMES are unaffected by
    -- this -- those stay exactly as today (pipeline-stage boundaries, not
    -- domain boundaries); domain identity is expressed via table_name's
    -- naming convention above, and via which dbt project a model's files
    -- physically live in, not via a separate physical schema per domain.
    model_schema          text not null,
    batch_hierarchy       int not null default 0,
    table_type            text not null check (table_type in ('fact', 'dimension')),
    business_key_columns  jsonb not null default '[]'::jsonb,
    -- attribute columns hash-compared via _attr_hash to detect a Type 2
    -- new-version or Type 1 in-place update
    tracked_columns       jsonb not null default '[]'::jsonb,
    scd_type              smallint not null default 2 check (scd_type in (1, 2)),
    -- also drives whether this model's upstream staging source(s) merge on
    -- attribute change -- see metadata/DataModel.md "Staging update-tracking rule"
    updates_enabled       boolean not null default true,
    deletes_enabled       boolean not null default false,
    watermark_column      text,
    load_type             smallint not null references load_type(id),
    -- comma-separated data_feed.id values that must succeed before this
    -- model builds; replaces both staging_source_data_feed_id and the
    -- deleted model_feed_source bridge table
    depends_on_feeds      text,
    -- which single feed's per-feed Dagster job/dbt build actually claims
    -- this model's AssetKey (must be one of depends_on_feeds, enforced at
    -- the application layer -- same as depends_on_feeds itself, not a real
    -- FK-in-a-list constraint). Real FK, not a loose comma-list, because
    -- exactly one owner is a hard Dagster requirement, not a soft
    -- convention: two @dbt_assets Python functions both claiming the same
    -- AssetKey is a hard error, not just undesirable. Required even for a
    -- single-feed model (trivially equal to that feed) so the meaning is
    -- always well-defined, never implicit. See Learnings.md, "A dbt model
    -- tagged with two feed tags gets claimed by two competing @dbt_assets
    -- defs" for why this exists.
    owning_feed_id        uuid not null references data_feed(id),
    -- comma-separated pipeline_steps.id values -- a model has no extraction
    -- of its own (that belongs to the feed(s) it depends on), so this only
    -- ever meaningfully gates 'serving' (1,2 = transformation+serving by
    -- default). Resolved at codegen time by generate_serve_views.py, not
    -- live per-run (see the master pipeline addendum) -- a model's own
    -- serve views simply aren't generated when serving isn't selected.
    pipeline_steps        text not null default '1,2',
    -- denormalized watermark state for the orchestrator; data_processing_runs is the full run history
    last_watermark_value  text,
    last_run_id           uuid,
    is_active             boolean not null default true
);

-- ---------------------------------------------------------------------------
-- ingestion_triggers (metadata for how a feed/model's master_pipeline run
-- actually gets kicked off -- a cron schedule, or a storage/sensor trigger
-- watching a feed's own landing directory for a new file. A build-time
-- codegen step, scripts/generate_dagster_pipeline.py, reads this table and
-- constructs the real Dagster ScheduleDefinition/SensorDefinition objects.
-- Renamed from the original `schedule` table, which only covered the cron
-- case -- see Roadmap.md/Backlog.md for the generalization.)
-- ---------------------------------------------------------------------------
create table ingestion_triggers (
    id                        uuid primary key default gen_random_uuid(),
    trigger_type              text not null check (trigger_type in ('schedule', 'sensor')),
    -- only meaningful (and only required) for a schedule-type trigger
    cron                      text,
    -- polymorphic: a data_feed.id or a lakehouse_models.id, depending on controlling_object_type
    controlling_object_id     uuid not null,
    controlling_object_type   text not null check (controlling_object_type in ('model', 'feed')),
    is_active                 boolean not null default true,
    constraint chk_ingestion_triggers_cron check (
        trigger_type <> 'schedule' or cron is not null
    ),
    -- a sensor watches a feed's own landing directory -- a model has no
    -- source/landing concept of its own, so sensor-type is feed-only.
    -- Row-local (both columns live here), so enforced in the DB in
    -- addition to the frontend -- unlike the sensor/connector_kind
    -- eligibility check below, which reaches source_system through two
    -- joins and can only ever be an application-layer check.
    constraint chk_ingestion_triggers_sensor_feed_only check (
        trigger_type <> 'sensor' or controlling_object_type = 'feed'
    )
);

-- at most one trigger per controlled feed/model -- a feed/model picks
-- schedule OR sensor, never both at once; not an oversight. Also what makes
-- idempotent seeding possible (every other table's seed function uses ON
-- CONFLICT against a natural-key unique constraint).
create unique index uq_ingestion_triggers_controlling_object
    on ingestion_triggers (controlling_object_type, controlling_object_id);

-- ---------------------------------------------------------------------------
-- data_processing_runs (one row per feed-run or model-run per job execution
-- -- same grain as the former data_feed_run/data_model_run, merged into one
-- wide table spanning landing -> raw -> clean -> staging -> model -> serve.
-- See metadata/DataModel.md for the merge rationale.)
-- ---------------------------------------------------------------------------
create table data_processing_runs (
    run_id                          uuid primary key default gen_random_uuid(),
    -- populated for a feed-run row
    data_feed_id                    uuid references data_feed(id),
    -- populated for a model-run row; corresponds to lakehouse_models.friendly_name (not a real FK)
    model_key                       text,
    -- comma-separated data_feed.friendly_name values, populated alongside model_key
    uses_feeds                      text,
    -- either a batch_group value or a model_schema value, depending on tracking_group_type
    tracking_group                  text not null,
    tracking_group_type             text not null check (tracking_group_type in ('batch_group', 'model_schema')),
    -- The master pipeline's own dagster_run_id (Roadmap.md "Master pipeline
    -- orchestration") -- created once, by the master pipeline job, before
    -- any of the three child stage jobs run. The three columns below
    -- record each stage's own separate dagster_run_id once that stage's
    -- job actually executes -- a child stage job is a genuinely different
    -- Dagster run from the master and from its sibling stages, so one
    -- logical pipeline execution now spans up to four distinct run ids,
    -- not one. Extraction covers raw+clean as one job/run (raw and clean
    -- are tightly coupled -- raw exists specifically to feed clean,
    -- nothing else consumes it -- so there is no separate "validation" run
    -- id column); the five schema-level column groups below
    -- (is_raw_successful/is_clean_successful/etc.) stay fully independent
    -- of each other regardless, preserving exactly the granularity a
    -- future rerun-just-raw-to-clean feature would need, even though this
    -- doesn't build that feature.
    master_dagster_run_id           text not null,
    -- The watermark folder path (YYYY/MM/DD/HH/MI/SS) this run's raw data is
    -- extracted into -- generated once, at row-creation time (the master
    -- pipeline's record_run_started()), not derived later from
    -- extraction_dagster_run_id. Pins the raw read path unambiguously: the
    -- clean step reads from exactly this path rather than relying on
    -- context.run_id parity between the raw and clean steps of the same
    -- job. Populated for feed-run rows only -- a model-run row never
    -- touches raw. See metadata/DataModel.md.
    storage_watermark               text,
    extraction_dagster_run_id       text,
    transformation_dagster_run_id   text,
    serving_dagster_run_id          text,
    job_started_timestamp           timestamptz not null default now(),
    job_ended_timestamp             timestamptz,
    job_successful                  boolean,

    -- No "landing" stage column group -- "landing" was never a real
    -- pipeline concept (see Roadmap.md's terminology cleanup), just a
    -- historical mislabeling of the fetch sub-step within extraction. Its
    -- role (fetch outcome, watermark tracking) folds into raw's own
    -- columns below, since raw is the legitimate storage-layer name for
    -- what the fetch actually produces durably.
    is_raw_successful               boolean,
    raw_end_timestamp               timestamptz,
    raw_error_message               text,
    raw_rows_read                   bigint,
    raw_rows_inserted               bigint,
    raw_rows_updated                bigint,
    raw_rows_deleted                bigint,
    raw_output_path                 text,
    raw_watermark_value_start       text,
    raw_watermark_value_end         text,

    is_clean_successful             boolean,
    clean_end_timestamp             timestamptz,
    clean_error_message             text,
    clean_rows_read                 bigint,
    clean_rows_inserted             bigint,
    clean_rows_updated              bigint,
    clean_rows_deleted              bigint,
    clean_output_path               text,
    clean_watermark_value_start     text,
    clean_watermark_value_end       text,

    is_staging_successful           boolean,
    staging_end_timestamp           timestamptz,
    staging_error_message           text,
    staging_rows_read               bigint,
    staging_rows_inserted           bigint,
    staging_rows_updated            bigint,
    staging_rows_deleted            bigint,
    staging_output_path             text,
    staging_watermark_value_start   text,
    staging_watermark_value_end     text,

    is_model_successful             boolean,
    model_end_timestamp             timestamptz,
    model_error_message             text,
    model_rows_read                 bigint,
    model_rows_inserted             bigint,
    model_rows_updated              bigint,
    model_rows_deleted              bigint,
    model_output_path               text,
    model_watermark_value_start     text,
    model_watermark_value_end       text,

    is_serve_successful             boolean,
    serve_end_timestamp             timestamptz,
    serve_error_message             text,
    serve_rows_read                 bigint,
    serve_rows_inserted             bigint,
    serve_rows_updated              bigint,
    serve_rows_deleted              bigint,
    serve_output_path               text,
    serve_watermark_value_start     text,
    serve_watermark_value_end       text,

    created_at                      timestamptz not null default now(),

    constraint chk_data_processing_runs_one_target check (
        (data_feed_id is not null and model_key is null) or
        (data_feed_id is null and model_key is not null)
    )
);

create unique index uq_data_processing_runs_feed
    on data_processing_runs (data_feed_id, master_dagster_run_id)
    where data_feed_id is not null;

create unique index uq_data_processing_runs_model
    on data_processing_runs (model_key, master_dagster_run_id)
    where model_key is not null;

create index idx_data_processing_runs_feed_started
    on data_processing_runs (data_feed_id, job_started_timestamp desc);

create index idx_data_processing_runs_model_started
    on data_processing_runs (model_key, job_started_timestamp desc);

create index idx_data_processing_runs_master_dagster_run
    on data_processing_runs (master_dagster_run_id);
