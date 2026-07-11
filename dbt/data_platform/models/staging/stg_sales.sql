{{
  config(
    unique_key='_key_hash',
    alias='sales',
    tags=['sales']
  )
}}

{#
    clean -> staging: cumulative insert/update, hash-gated (see
    Roadmap.md "Model Layer: SCD Design"). Same insert/update-split
    pattern as stg_customers.sql — see there for the full reasoning
    (why this isn't a MERGE, why every column is cast explicitly).
    sales is transactional (each invoice_id is immutable once written), so
    in practice _attr_hash should never change for an existing key, but
    the classification costs nothing and stays consistent with every
    other staging model.
#}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with source_raw as (

    select
        cast(invoice_id as varchar) as invoice_id,
        cast(branch as varchar) as branch,
        cast(city as varchar) as city,
        cast(customer_type as varchar) as customer_type,
        cast(gender as varchar) as gender,
        cast(product_line as varchar) as product_line,
        cast(unit_price as double) as unit_price,
        cast(quantity as bigint) as quantity,
        cast(tax_amount as double) as tax_amount,
        cast(total as double) as total,
        cast(payment_method as varchar) as payment_method,
        cast(cogs as double) as cogs,
        cast(gross_income as double) as gross_income,
        cast(rating as double) as rating,
        cast(sale_timestamp as timestamp(6) with time zone) as sale_timestamp,
        {{ row_hash(['invoice_id']) }} as _key_hash,
        {{ row_hash(['branch', 'city', 'customer_type', 'gender', 'product_line', 'unit_price', 'quantity', 'tax_amount', 'total', 'payment_method', 'cogs', 'gross_income', 'rating', 'sale_timestamp']) }} as _attr_hash
    from {{ source('clean', 'sales') }}

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
