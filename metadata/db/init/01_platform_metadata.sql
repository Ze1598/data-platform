-- Platform metadata schema: source_system, data_feed, schema_registry,
-- model_feed, model_feed_source, data_feed_run, data_model_run.
-- See Roadmap.md "Metadata Schema" for the design rationale.

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
    connection_config  jsonb not null default '{}'::jsonb,
    is_active          boolean not null default true,
    created_at         timestamptz not null default now(),
    updated_at         timestamptz not null default now()
);

create trigger trg_source_system_updated_at
    before update on source_system
    for each row execute function set_updated_at();

-- ---------------------------------------------------------------------------
-- data_feed (one row per source object/table/endpoint)
-- ---------------------------------------------------------------------------
create table data_feed (
    id                        uuid primary key default gen_random_uuid(),
    source_system_id          uuid not null references source_system(id),
    code                      text not null unique,
    name                      text not null,
    object_name               text not null,
    extraction_type           text not null check (extraction_type in ('full', 'incremental')),
    incremental_column        text,
    incremental_column_type   text,
    extraction_config         jsonb not null default '{}'::jsonb,
    landing_path_template     text,
    raw_path_template         text,
    business_key_columns      jsonb not null default '[]'::jsonb,
    staging_table_name        text,
    schedule_cron             text,
    -- which engine runs this feed's raw->clean transform: 'polars' by
    -- default (runs inline in the Dagster op, no extra cluster
    -- infrastructure), 'spark' opt-in for feeds whose volume actually
    -- needs distributed execution (see Learnings.md, Phase 6)
    processing_engine         text not null default 'polars' check (processing_engine in ('polars', 'spark')),
    -- mirrors model_feed.updates_enabled -- staging's clean->staging merge
    -- is keyed off data_feed, not model_feed, so it needs its own copy of
    -- this flag rather than reading model_feed's (which governs the
    -- separate staging->model layer). false means this feed is treated as
    -- insert-only in staging: attribute-hash change detection is skipped
    -- entirely, only new business keys are ever written.
    updates_enabled           boolean not null default true,
    -- denormalized watermark state for the orchestrator; data_feed_run is the full run history
    last_watermark_value      text,
    last_run_id               uuid,
    is_active                 boolean not null default true,
    created_at                timestamptz not null default now(),
    updated_at                timestamptz not null default now(),
    constraint chk_data_feed_incremental_column check (
        extraction_type = 'full' or incremental_column is not null
    )
);

create index idx_data_feed_source_system on data_feed (source_system_id);

create trigger trg_data_feed_updated_at
    before update on data_feed
    for each row execute function set_updated_at();

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
-- model_feed (fact/dim config)
-- ---------------------------------------------------------------------------
create table model_feed (
    id                            uuid primary key default gen_random_uuid(),
    code                          text not null unique,
    model_type                    text not null check (model_type in ('fact', 'dimension')),
    staging_source_data_feed_id   uuid not null references data_feed(id),
    business_key_columns          jsonb not null default '[]'::jsonb,
    tracked_columns               jsonb not null default '[]'::jsonb,
    surrogate_key_column          text not null default '_scd_id',
    scd_type                      smallint not null default 2 check (scd_type in (1, 2)),
    updates_enabled               boolean not null default true,
    deletions_enabled             boolean not null default false,
    watermark_column              text,
    -- denormalized watermark state for the orchestrator; data_model_run is the full run history
    last_watermark_value          text,
    last_run_id                   uuid,
    is_active                     boolean not null default true,
    created_at                    timestamptz not null default now(),
    updated_at                    timestamptz not null default now()
);

create index idx_model_feed_staging_source on model_feed (staging_source_data_feed_id);

create trigger trg_model_feed_updated_at
    before update on model_feed
    for each row execute function set_updated_at();

-- ---------------------------------------------------------------------------
-- model_feed_source (bridge table for facts joining >1 staging source)
-- NOT used for tracking which feed(s) a staging build draws from -- that's
-- data_model_run.uses_feeds (a plain comma-separated column), deliberately
-- separate. model_feed itself is Kimball-specific (model_type constrained
-- to fact/dimension, requires scd_type/surrogate_key_column/business_key_
-- columns/tracked_columns), and staging isn't a fact or a dimension, so it
-- doesn't have a model_feed row to bridge from in the first place. Don't
-- "fix" data_model_run to route through here -- that was considered and
-- rejected, see Learnings.md.
-- ---------------------------------------------------------------------------
create table model_feed_source (
    model_feed_id   uuid not null references model_feed(id),
    data_feed_id    uuid not null references data_feed(id),
    role            text not null default 'primary',
    primary key (model_feed_id, data_feed_id)
);

-- ---------------------------------------------------------------------------
-- data_feed_run (one row per data_feed per job run — extraction + contract
-- validation concern: landing -> raw -> clean. See Roadmap.md "Metadata
-- Schema" and Learnings.md for why this replaced a per-layer audit log.)
-- ---------------------------------------------------------------------------
create table data_feed_run (
    run_id                          uuid primary key default gen_random_uuid(),
    data_feed_id                    uuid not null references data_feed(id),
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

    created_at                      timestamptz not null default now(),
    unique (data_feed_id, dagster_run_id)
);

create index idx_data_feed_run_feed on data_feed_run (data_feed_id, job_started_timestamp desc);
create index idx_data_feed_run_dagster_run on data_feed_run (dagster_run_id);

-- ---------------------------------------------------------------------------
-- data_model_run (one row per model unit per job run — warehouse-building
-- concern: staging -> model -> serve. Not FK'd to model_feed: staging is
-- built per data_feed today with no model_feed involved at all, and
-- model/serve don't exist yet (Phase 7/8). `model_key` names the model
-- unit being built (e.g. 'customers' today, matching stg_customers; will
-- align with model_feed.code once Phase 7 introduces real rows).
-- `uses_feeds` is a comma-separated list of data_feed codes this model
-- unit draws from — one feed per staging model today, built to support a
-- future fact model joining multiple staging sources.
-- ---------------------------------------------------------------------------
create table data_model_run (
    run_id                          uuid primary key default gen_random_uuid(),
    model_key                       text not null,
    uses_feeds                      text not null,
    dagster_run_id                  text not null,
    job_started_timestamp           timestamptz not null default now(),
    job_ended_timestamp             timestamptz,
    job_successful                  boolean,

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
    unique (model_key, dagster_run_id)
);

create index idx_data_model_run_model_key on data_model_run (model_key, job_started_timestamp desc);
create index idx_data_model_run_dagster_run on data_model_run (dagster_run_id);
