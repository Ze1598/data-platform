{{
  config(
    unique_key='_key_hash',
    alias='iot_telemetry_dim_device',
    tags=['device_heartbeats']
  )
}}

{#
    Hand-owned staging for iot_telemetry_dim_device, distinct from
    stg_device_heartbeats -- aliased by the model's own table_name since
    it predates this platform's per-feed staging convention. No metadata
    dependency: casts and the key/tracked column split are plain
    hand-written business logic, same as any other staging model.
#}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with device_heartbeats_raw as (

    select
        cast(device_id as varchar) as device_id,
        cast(battery_level as bigint) as battery_level,
        cast(signal_strength as bigint) as signal_strength,
        cast(firmware_version as varchar) as firmware_version,
        cast(from_iso8601_timestamp(ts) at time zone 'UTC' as timestamp(6) with time zone) as ts
    from {{ source('clean', 'device_heartbeats') }}

),

source_raw as (

    select
        *,
        {{ row_hash(['device_id']) }} as _key_hash,
        {{ row_hash(['battery_level', 'signal_strength', 'firmware_version']) }} as _attr_hash
    from device_heartbeats_raw

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
