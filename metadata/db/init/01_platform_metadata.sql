-- Platform metadata schema: source_system, data_feed, schema_registry,
-- model_feed, model_feed_source, run_audit_log.
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
    -- denormalized watermark state for the orchestrator; run_audit_log is the full audit trail
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
    -- denormalized watermark state for the orchestrator; run_audit_log is the full audit trail
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
-- ---------------------------------------------------------------------------
create table model_feed_source (
    model_feed_id   uuid not null references model_feed(id),
    data_feed_id    uuid not null references data_feed(id),
    role            text not null default 'primary',
    primary key (model_feed_id, data_feed_id)
);

-- ---------------------------------------------------------------------------
-- run_audit_log (per-layer run/audit history — idempotency + re-run support)
-- ---------------------------------------------------------------------------
create table run_audit_log (
    run_id                uuid primary key default gen_random_uuid(),
    layer                 text not null check (layer in ('landing', 'raw', 'clean', 'staging', 'model', 'serve')),
    feed_type             text not null check (feed_type in ('data_feed', 'model_feed')),
    data_feed_id          uuid references data_feed(id),
    model_feed_id         uuid references model_feed(id),
    parent_run_id         uuid references run_audit_log(run_id),
    watermark_value_start text,
    watermark_value_end   text,
    output_path           text,
    status                text not null default 'running' check (status in ('running', 'success', 'failed', 'skipped')),
    started_at            timestamptz not null default now(),
    ended_at              timestamptz,
    rows_read             bigint,
    rows_inserted         bigint,
    rows_updated          bigint,
    rows_deleted          bigint,
    error_message         text,
    dagster_run_id        text,
    created_at            timestamptz not null default now(),
    constraint chk_run_audit_log_feed_ref check (
        (feed_type = 'data_feed' and data_feed_id is not null and model_feed_id is null)
        or (feed_type = 'model_feed' and model_feed_id is not null and data_feed_id is null)
    )
);

create index idx_run_audit_log_data_feed on run_audit_log (layer, feed_type, data_feed_id, started_at desc);
create index idx_run_audit_log_model_feed on run_audit_log (layer, feed_type, model_feed_id, started_at desc);
