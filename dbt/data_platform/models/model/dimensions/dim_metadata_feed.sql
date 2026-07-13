{{ config(schema='model', unique_key='_key_hash', alias='dim_metadata_feed', tags=['metadata_runs']) }}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with base as (
    select distinct
        feed_friendly_name, feed_batch_group_friendly_name,
        feed_extraction_type, feed_processing_engine, feed_is_active,
        false as is_deleted
    from {{ ref('stg_metadata_runs') }}
    where feed_friendly_name is not null
),
hashed as (
    select *,
        {{ row_hash(['feed_friendly_name']) }} as _key_hash,
        {{ row_hash(['feed_batch_group_friendly_name', 'feed_extraction_type', 'feed_processing_engine', 'feed_is_active', 'is_deleted']) }} as _attr_hash
    from base
)
{% if is_incremental() %}
, to_merge as ({{ classify_changes('hashed', updates_enabled) }})
{% endif %}
select *,
    cast(null as varchar) as _scd_id, cast(null as timestamp(6)) as _valid_from,
    cast(null as timestamp(6)) as _valid_to, {{ dbt.current_timestamp() }} as _updated_at
from {{ 'to_merge' if is_incremental() else 'hashed' }}
