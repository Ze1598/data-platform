{{ config(schema='model', unique_key='_key_hash', alias='metadata_fct_runs', tags=['metadata_runs']) }}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with base as (
    select
        run_id, data_feed_id, model_key, master_dagster_run_id,
        extraction_dagster_run_id, transformation_dagster_run_id,
        serving_dagster_run_id, tracking_group, tracking_group_type,
        job_started_timestamp, job_ended_timestamp, job_successful,
        raw_rows_read, clean_rows_inserted,
        staging_rows_updated, model_rows_updated, serve_rows_read,
        false as is_deleted
    from {{ ref('stg_metadata_runs') }}
),
hashed as (
    select *,
        {{ row_hash(['run_id']) }} as _key_hash,
        {{ row_hash(['job_successful', 'job_ended_timestamp', 'raw_rows_read', 'clean_rows_inserted', 'staging_rows_updated', 'model_rows_updated', 'serve_rows_read', 'is_deleted']) }} as _attr_hash
    from base
)
{% if is_incremental() %}
, to_merge as ({{ classify_changes('hashed', updates_enabled) }})
{% endif %}
select *,
    cast(null as varchar) as _scd_id, cast(null as timestamp(6)) as _valid_from,
    cast(null as timestamp(6)) as _valid_to, {{ dbt.current_timestamp() }} as _updated_at
from {{ 'to_merge' if is_incremental() else 'hashed' }}