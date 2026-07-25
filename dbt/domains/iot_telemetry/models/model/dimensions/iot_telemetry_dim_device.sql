{{
  config(
    schema='model',
    unique_key='_key_hash',
    alias='iot_telemetry_dim_device',
    tags=['device_heartbeats']
  )
}}

{#
    TODO: describe this model's real business logic here.
    Generated scaffold (scripts/generate_model_scaffolds.py) -- `base`
    below is pre-filled from lakehouse_models' business_key_columns/
    tracked_columns only. Verify the column names/source, and add any
    joins this model needs (e.g. a dimensional FK via another model's
    _key_hash -- see an existing fct_*.sql for the pattern; that join
    can't be auto-derived, no metadata describes it).

    friendly_name (display label): dim_iot_device
    business_key_columns: ['device_id']
    tracked_columns:      ['battery_level', 'signal_strength', 'firmware_version']
    is_deleted:            false as is_deleted (deletes_enabled=false)
#}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with base as (

    -- TODO: verify/adjust -- replace with the real business-logic select.
    select
        device_id,
        battery_level,
        signal_strength,
        firmware_version,
        false as is_deleted
    from {{ ref('stg_iot_telemetry_dim_device') }}

),

hashed as (

    select
        *,
        {{ row_hash(['device_id']) }} as _key_hash,
        {{ row_hash(['battery_level', 'signal_strength', 'firmware_version', 'is_deleted']) }} as _attr_hash
    from base

)

{% if is_incremental() %}

, to_merge as (
    {{ classify_changes('hashed', updates_enabled) }}
)

{% endif %}

select
    *,
    cast(null as varchar) as _scd_id,
    cast(null as timestamp(6)) as _valid_from,
    cast(null as timestamp(6)) as _valid_to,
    {{ dbt.current_timestamp() }} as _updated_at
from {{ 'to_merge' if is_incremental() else 'hashed' }}
