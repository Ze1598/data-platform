{{
  config(
    unique_key='_key_hash',
    alias='customers',
    tags=['customers']
  )
}}

{#
    clean -> staging: cumulative insert/update, hash-gated (see Roadmap.md
    "Model Layer: SCD Design"). Explicit insert/update split, not a MERGE
    (see Learnings.md for the full reasoning): classify_changes() (a
    single join against the target) classifies each surviving row as
    'insert' (no matching key in target) or 'update' (matching key,
    different _attr_hash) via `_change_type`; insert_update_split.sql's
    `trino__get_delete_insert_merge_sql` override applies the two sets
    with their own DELETE+INSERT statements. That join *is* the
    change-detection step -- not a pre-filter ahead of a second join
    inside a generated MERGE, which is what this replaced.

    `updates_enabled` comes from data_feed.updates_enabled via `--vars`
    (dbt_assets.py), not read live from Postgres inside this SQL (Trino
    has no catalog federating into platform_metadata) -- when false, the
    `or target._attr_hash != ...` branch is omitted at compile time, so
    this feed is treated as insert-only: an existing business key never
    gets re-evaluated for attribute changes, only genuinely new keys are
    ever written.

    Explicit casts on every source column: staging is the warehouse's
    stable contract, decoupled from whatever type `clean` happens to infer
    this run. raw_to_clean's schema evolution (schema_registry) lets
    clean's column types drift when the source genuinely changes (e.g. an
    all-digit code inferring as an integer) -- without a cast here, that
    drift breaks this model's own already-materialized column types the
    moment clean's type no longer matches. Pinning the type here is the
    intentional point where a genuine upstream schema change gets a
    deliberate human decision about the warehouse-facing type, rather than
    silently inheriting whatever clean happened to infer.
#}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with source_raw as (

    select
        cast(customer_id as bigint) as customer_id,
        cast(name as varchar) as name,
        cast(email as varchar) as email,
        cast(updated_at as timestamp(6) with time zone) as updated_at,
        {{ row_hash(['customer_id']) }} as _key_hash,
        {{ row_hash(['name', 'email', 'updated_at']) }} as _attr_hash
    from {{ source('clean', 'customers') }}

)

{% if is_incremental() %}

, source as (
    {{ classify_changes('source_raw', updates_enabled) }}
)

{% endif %}

select
    *,
    {{ dbt.current_timestamp() }} as _loaded_at
from {{ 'source' if is_incremental() else 'source_raw' }}
