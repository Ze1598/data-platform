{{
  config(
    schema='model',
    unique_key='_key_hash',
    alias='fct_sales',
    tags=['sales']
  )
}}

{#
    Fact table, Type 1-style insert/update split (same pattern as
    dim_branch.sql/stg_customers.sql -- see there for the reasoning).
    Joins to dim_branch on the natural branch key to pull in its
    _key_hash as the dimensional FK. Deliberately joins on _key_hash, not
    _scd_id: _scd_id is Type-2-specific (a new one per version) and always
    null on dim_branch, a Type 1 dimension with no history to version --
    Roadmap.md's "join to _scd_id" applies when the target dimension is
    actually Type 2 (see Learnings.md for why sales has no such join
    today: no customer FK in this dataset).
#}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with base as (

    select
        s.invoice_id,
        s.customer_type,
        s.gender,
        s.product_line,
        s.unit_price,
        s.quantity,
        s.tax_amount,
        s.total,
        s.payment_method,
        s.cogs,
        s.gross_income,
        s.rating,
        s.sale_timestamp,
        b._key_hash as branch_key,
        false as is_deleted
    from {{ ref('stg_sales') }} s
    left join {{ ref('dim_branch') }} b on s.branch = b.branch

),

hashed as (

    select
        *,
        {{ row_hash(['invoice_id']) }} as _key_hash,
        {{ row_hash(['unit_price', 'quantity', 'tax_amount', 'total', 'cogs', 'gross_income', 'rating', 'is_deleted']) }} as _attr_hash
    from base

)

{% if is_incremental() %}

, to_merge as (

    select
        hashed.*,
        case when target._key_hash is null then 'insert' else 'update' end as _change_type
    from hashed
    left join {{ this }} as target
        on hashed._key_hash = target._key_hash
    where target._key_hash is null                                   -- new invoice
       {% if updates_enabled %}
       or target._attr_hash != hashed._attr_hash                     -- changed attributes
       {% endif %}

)

{% endif %}

select
    *,
    cast(null as varchar) as _scd_id,
    cast(null as timestamp(6)) as _valid_from,
    cast(null as timestamp(6)) as _valid_to,
    {{ dbt.current_timestamp() }} as _updated_at
from {{ 'to_merge' if is_incremental() else 'hashed' }}
