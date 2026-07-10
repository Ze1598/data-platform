{{
  config(
    unique_key='_key_hash',
    alias='sales',
    tags=['sales']
  )
}}

{#
    clean -> staging: cumulative upsert, hash-gated (see Roadmap.md "Model
    Layer: SCD Design"). Same pattern as stg_customers.sql — see there for
    the anti-join reasoning. sales is transactional (each invoice_id is
    immutable once written), so in practice _attr_hash should never change
    for an existing key, but the gate costs nothing and stays consistent
    with every other staging model.
#}

with source_raw as (

    select
        invoice_id,
        branch,
        city,
        customer_type,
        gender,
        product_line,
        unit_price,
        quantity,
        tax_amount,
        total,
        payment_method,
        cogs,
        gross_income,
        rating,
        sale_timestamp,
        {{ row_hash(['invoice_id']) }} as _key_hash,
        {{ row_hash(['branch', 'city', 'customer_type', 'gender', 'product_line', 'unit_price', 'quantity', 'tax_amount', 'total', 'payment_method', 'cogs', 'gross_income', 'rating', 'sale_timestamp']) }} as _attr_hash
    from {{ source('clean', 'sales') }}

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
