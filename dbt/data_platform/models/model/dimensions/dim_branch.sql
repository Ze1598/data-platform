{{
  config(
    schema='model',
    unique_key='_key_hash',
    alias='dim_branch',
    tags=['sales']
  )
}}

{#
    Type 1 dimension, conformed out of sales' own branch/city columns --
    sales has no real FK to a separate branch source, so this dimension is
    extracted directly from the fact source itself (see Learnings.md for
    why: sales has no customer FK either, so fct_sales joins here instead
    of to dim_customer). Same anti-join-gate pattern as stg_customers.sql
    (see there for the reasoning) rather than a hand-written MERGE ... WHEN
    MATCHED AND condition -- one mechanism for "only write changed rows",
    reused everywhere it's needed.
#}

with base as (

    select distinct
        branch,
        city,
        false as is_deleted
    from {{ ref('stg_sales') }}

),

hashed as (

    select
        branch,
        city,
        is_deleted,
        {{ row_hash(['branch']) }} as _key_hash,
        {{ row_hash(['city', 'is_deleted']) }} as _attr_hash
    from base

)

{% if is_incremental() %}

, to_merge as (

    select hashed.*
    from hashed
    left join {{ this }} as target
        on hashed._key_hash = target._key_hash
    where target._key_hash is null                       -- new branch
       or target._attr_hash != hashed._attr_hash          -- changed attributes

)

{% endif %}

select
    *,
    cast(null as varchar) as _scd_id,
    cast(null as timestamp(6)) as _valid_from,
    cast(null as timestamp(6)) as _valid_to,
    {{ dbt.current_timestamp() }} as _updated_at
from {{ 'to_merge' if is_incremental() else 'hashed' }}
