{{
  config(
    unique_key='_key_hash',
    alias='customers',
    tags=['customers']
  )
}}

{#
    clean -> staging: cumulative upsert, hash-gated (see Roadmap.md "Model
    Layer: SCD Design"). The anti-join below excludes rows whose _attr_hash
    hasn't changed *before* the merge ever sees them, rather than relying on
    the merge's WHEN MATCHED clause to no-op — portable across adapters, and
    means an unchanged row produces zero writes, not a same-value UPDATE.
#}

with source_raw as (

    select
        customer_id,
        name,
        email,
        updated_at,
        {{ row_hash(['customer_id']) }} as _key_hash,
        {{ row_hash(['name', 'email', 'updated_at']) }} as _attr_hash
    from {{ source('clean', 'customers') }}

)

{% if is_incremental() %}

, source as (

    select source_raw.*
    from source_raw
    left join {{ this }} as target
        on source_raw._key_hash = target._key_hash
    where target._key_hash is null                       -- new business key
       or target._attr_hash != source_raw._attr_hash      -- changed attributes
)

{% endif %}

select
    *,
    {{ dbt.current_timestamp() }} as _loaded_at
from {{ 'source' if is_incremental() else 'source_raw' }}
