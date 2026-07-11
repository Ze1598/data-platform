{{
  config(
    unique_key='_key_hash',
    alias='financial_transactions',
    tags=['financial_transactions']
  )
}}

{#
    clean -> staging, same insert/update-split pattern as
    stg_customers.sql (see there for the full reasoning, including why
    this isn't a MERGE and why every column is cast explicitly).
    financial_transactions is Phase 9's incremental CSV file-drop feed --
    clean only ever holds *this run's* new batch (see financial_assets.py),
    staging is what accumulates the full history across runs by business
    key. account_code is the concrete case that motivated the explicit
    casts here: it's a chart-of-accounts identifier (varchar by intent),
    but its values are all-digit, so Polars/clean's schema inference can
    legitimately land on a numeric type depending on what a given batch
    looks like -- the cast pins staging to the intended varchar
    regardless.

    Note: a posted financial transaction is real-world immutable (you
    post a reversing entry, you don't edit a posted GL line) -- this feed
    is a plausible candidate for data_feed.updates_enabled=false
    (insert-only), but that's a metadata decision left to whoever owns
    this feed's config, not decided here; the mechanism just respects
    whatever the flag says.
#}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with source_raw as (

    select
        cast(transaction_id as varchar) as transaction_id,
        cast(posted_date as timestamp(6) with time zone) as posted_date,
        cast(account_code as varchar) as account_code,
        cast(account_name as varchar) as account_name,
        cast(description as varchar) as description,
        cast(debit_amount as double) as debit_amount,
        cast(credit_amount as double) as credit_amount,
        cast(currency as varchar) as currency,
        cast(cost_center as varchar) as cost_center,
        {{ row_hash(['transaction_id']) }} as _key_hash,
        {{ row_hash(['posted_date', 'account_code', 'account_name', 'description', 'debit_amount', 'credit_amount', 'currency', 'cost_center']) }} as _attr_hash
    from {{ source('clean', 'financial_transactions') }}

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
