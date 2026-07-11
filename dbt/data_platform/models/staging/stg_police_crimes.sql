{{
  config(
    unique_key='_key_hash',
    alias='police_crimes',
    tags=['police_crimes']
  )
}}

{#
    clean -> staging, same insert/update-split pattern as
    stg_customers.sql (see there for the full reasoning). police_crimes
    is Phase 9's incremental UK Police API feed -- clean only ever holds
    *this run's* new crimes, which can span several months in one run
    (landing_police_crimes pulls everything since the last watermark
    through the latest available month, see police_assets.py), staging
    accumulates every month ever pulled by business key (the crime's own
    numeric id). Note: a given crime's outcome_category/outcome_date can
    genuinely change between API pulls of the same month (an
    investigation concludes after the fact) -- this is exactly the case
    updates_enabled must stay true for; the _attr_hash comparison
    correctly picks that up as a real attribute change if this feed is
    ever re-pulled for an already-seen month, not just treated as a
    duplicate.

    Explicit casts on every source column -- see stg_customers.sql for why
    (staging's stable contract vs. clean's evolving inferred types).
#}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with source_raw as (

    select
        cast(id as bigint) as id,
        cast(persistent_id as varchar) as persistent_id,
        cast(category as varchar) as category,
        cast(location_type as varchar) as location_type,
        cast(location_subtype as varchar) as location_subtype,
        cast(street_id as bigint) as street_id,
        cast(street_name as varchar) as street_name,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        cast(context as varchar) as context,
        cast(month as varchar) as month,
        cast(outcome_category as varchar) as outcome_category,
        cast(outcome_date as varchar) as outcome_date,
        {{ row_hash(['id']) }} as _key_hash,
        {{ row_hash(['persistent_id', 'category', 'location_type', 'location_subtype', 'street_id', 'street_name', 'latitude', 'longitude', 'context', 'month', 'outcome_category', 'outcome_date']) }} as _attr_hash
    from {{ source('clean', 'police_crimes') }}

)

{% if is_incremental() %}

, source as (

    select
        source_raw.*,
        case when target._key_hash is null then 'insert' else 'update' end as _change_type
    from source_raw
    left join {{ this }} as target
        on source_raw._key_hash = target._key_hash
    where target._key_hash is null                                   -- new business key
       {% if updates_enabled %}
       or target._attr_hash != source_raw._attr_hash                 -- changed attributes
       {% endif %}
)

{% endif %}

select
    *,
    {{ dbt.current_timestamp() }} as _loaded_at
from {{ 'source' if is_incremental() else 'source_raw' }}
