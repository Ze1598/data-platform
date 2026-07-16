{{
  config(
    unique_key='_key_hash',
    alias='metadata_runs',
    tags=['metadata_runs']
  )
}}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with source_raw as (

    select
        cast(run_id as varchar) as run_id,
        cast(data_feed_id as varchar) as data_feed_id,
        cast(model_key as varchar) as model_key,
        cast(tracking_group as varchar) as tracking_group,
        cast(tracking_group_type as varchar) as tracking_group_type,
        cast(dagster_run_id as varchar) as dagster_run_id,
        cast(job_started_timestamp as timestamp(6) with time zone) as job_started_timestamp,
        cast(job_ended_timestamp as timestamp(6) with time zone) as job_ended_timestamp,
        cast(job_successful as boolean) as job_successful,
        cast(landing_rows_read as bigint) as landing_rows_read,
        cast(raw_rows_read as bigint) as raw_rows_read,
        cast(clean_rows_inserted as bigint) as clean_rows_inserted,
        cast(staging_rows_updated as bigint) as staging_rows_updated,
        cast(model_rows_updated as bigint) as model_rows_updated,
        cast(serve_rows_read as bigint) as serve_rows_read,
        cast(feed_friendly_name as varchar) as feed_friendly_name,
        cast(feed_batch_group_friendly_name as varchar) as feed_batch_group_friendly_name,
        cast(feed_extraction_type as varchar) as feed_extraction_type,
        cast(feed_processing_engine as varchar) as feed_processing_engine,
        cast(feed_is_active as boolean) as feed_is_active,
        cast(model_friendly_name as varchar) as model_friendly_name,
        cast(model_model_schema as varchar) as model_model_schema,
        cast(model_table_type as varchar) as model_table_type,
        cast(model_scd_type as bigint) as model_scd_type,
        cast(model_updates_enabled as boolean) as model_updates_enabled,
        cast(model_deletes_enabled as boolean) as model_deletes_enabled,
        {{ row_hash(['run_id']) }} as _key_hash,
        {{ row_hash(['job_successful', 'job_ended_timestamp', 'landing_rows_read', 'raw_rows_read', 'clean_rows_inserted', 'staging_rows_updated', 'model_rows_updated', 'serve_rows_read']) }} as _attr_hash
    from {{ source('clean', 'metadata_runs') }}

)

{% if is_incremental() %}

, source as (
    {{ classify_changes('source_raw', updates_enabled) }}
)

{% endif %}

select *, {{ dbt.current_timestamp() }} as _loaded_at
from {{ 'source' if is_incremental() else 'source_raw' }}
