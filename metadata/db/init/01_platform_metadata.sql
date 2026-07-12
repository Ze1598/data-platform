-- Platform metadata schema: source_system, data_feed, schema_registry,
-- lakehouse_models, load_type, schedule, data_processing_runs.
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
    -- denormalized watermark state for the orchestrator; data_processing_runs is the full run history
    last_watermark_value      text,
    is_active                 boolean not null default true,
    constraint chk_data_feed_watermark_column check (
        extraction_type = 'full' or watermark_column is not null
    )
);

create index idx_data_feed_source_system on data_feed (source_system_id);

-- ---------------------------------------------------------------------------
-- schema_registry (versioned expected schema of each feed's clean output)
-- ---------------------------------------------------------------------------
create table schema_registry (
    id                   uuid primary key default gen_random_uuid(),
    data_feed_id         uuid not null references data_feed(id),
    version              int not null,
    column_definitions   jsonb not null,
    is_current           boolean not null default true,
    effective_from       timestamptz not null default now(),
    effective_to         timestamptz,
    created_at           timestamptz not null default now(),
    created_by           text,
    unique (data_feed_id, version)
);

-- only one current schema version per feed
create unique index uq_schema_registry_current
    on schema_registry (data_feed_id)
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
-- lakehouse_models (fact/dim config -- NOT staging; staging stays pure
-- naming-convention with no metadata row of its own)
-- ---------------------------------------------------------------------------
create table lakehouse_models (
    id                    uuid primary key default gen_random_uuid(),
    -- this is what dbt ref() resolves against, so uniqueness is load-bearing
    friendly_name         text not null unique,
    -- which Trino/Iceberg schema this table lands in
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
    -- denormalized watermark state for the orchestrator; data_processing_runs is the full run history
    last_watermark_value  text,
    last_run_id           uuid,
    is_active             boolean not null default true
);

-- ---------------------------------------------------------------------------
-- schedule (metadata for Dagster schedules; a build-time codegen step reads
-- this table and constructs the real Dagster ScheduleDefinition objects --
-- not yet built as a functioning consumer, only the table structure lands
-- in this pass)
-- ---------------------------------------------------------------------------
create table schedule (
    id                        uuid primary key default gen_random_uuid(),
    cron                      text not null,
    -- polymorphic: a data_feed.id or a lakehouse_models.id, depending on controlling_object_type
    controlling_object_id     uuid not null,
    controlling_object_type   text not null check (controlling_object_type in ('model', 'feed')),
    is_active                 boolean not null default true
);

-- at most one schedule per controlled feed/model -- also what makes idempotent
-- seeding possible (every other table's seed function uses ON CONFLICT against
-- a natural-key unique constraint; schedule had none until this one)
create unique index uq_schedule_controlling_object
    on schedule (controlling_object_type, controlling_object_id);

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
    dagster_run_id                  text not null,
    job_started_timestamp           timestamptz not null default now(),
    job_ended_timestamp             timestamptz,
    job_successful                  boolean,

    is_landing_successful           boolean,
    landing_end_timestamp           timestamptz,
    landing_error_message           text,
    landing_rows_read               bigint,
    landing_rows_inserted           bigint,
    landing_rows_updated            bigint,
    landing_rows_deleted            bigint,
    landing_output_path             text,
    landing_watermark_value_start   text,
    landing_watermark_value_end     text,

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
    on data_processing_runs (data_feed_id, dagster_run_id)
    where data_feed_id is not null;

create unique index uq_data_processing_runs_model
    on data_processing_runs (model_key, dagster_run_id)
    where model_key is not null;

create index idx_data_processing_runs_feed_started
    on data_processing_runs (data_feed_id, job_started_timestamp desc);

create index idx_data_processing_runs_model_started
    on data_processing_runs (model_key, job_started_timestamp desc);

create index idx_data_processing_runs_dagster_run
    on data_processing_runs (dagster_run_id);
