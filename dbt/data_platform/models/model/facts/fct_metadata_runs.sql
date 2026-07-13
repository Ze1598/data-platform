{{ config(schema='model', unique_key='_key_hash', alias='fct_metadata_runs', tags=['metadata_runs']) }}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with base as (
    select
        s.run_id, s.dagster_run_id, s.tracking_group, s.tracking_group_type,
        s.job_started_timestamp, s.job_ended_timestamp, s.job_successful,
        s.landing_rows_read, s.raw_rows_read, s.clean_rows_inserted,
        s.staging_rows_updated, s.model_rows_updated, s.serve_rows_read,
        f._key_hash as feed_key,
        m._key_hash as lakehouse_model_key,
        false as is_deleted
    from {{ ref('stg_metadata_runs') }} s
    left join {{ ref('dim_metadata_feed') }} f on s.feed_friendly_name = f.feed_friendly_name
    left join {{ ref('dim_metadata_model') }} m on s.model_friendly_name = m.model_friendly_name
),
hashed as (
    select *,
        {{ row_hash(['run_id']) }} as _key_hash,
        {{ row_hash(['job_successful', 'job_ended_timestamp', 'landing_rows_read', 'raw_rows_read', 'clean_rows_inserted', 'staging_rows_updated', 'model_rows_updated', 'serve_rows_read', 'is_deleted']) }} as _attr_hash
    from base
)
{% if is_incremental() %}
, to_merge as ({{ classify_changes('hashed', updates_enabled) }})
{% endif %}
select *,
    cast(null as varchar) as _scd_id, cast(null as timestamp(6)) as _valid_from,
    cast(null as timestamp(6)) as _valid_to, {{ dbt.current_timestamp() }} as _updated_at
from {{ 'to_merge' if is_incremental() else 'hashed' }}