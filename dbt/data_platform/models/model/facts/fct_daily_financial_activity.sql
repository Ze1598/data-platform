{{
  config(
    schema='model',
    unique_key='_key_hash',
    alias='fct_daily_financial_activity',
    tags=['financial_transactions']
  )
}}

{#
    Multi-feed test model (metadata redesign follow-on) -- row-grain UNION
    ALL of stg_sales + stg_financial_transactions, keyed by
    (source_feed, source_id). Exercises genuine multi-source ref() +
    lakehouse_models.depends_on_feeds=[sales, financial_transactions]
    gating; the business meaning is intentionally synthetic (these two
    feeds share no natural key) -- the point is proving the mechanics work
    for a real multi-feed fact, which is the expected shape in practice.
    Not aggregated: an aggregated daily total wouldn't compose with
    classify_changes()'s per-business-key incremental gating the same way
    a plain row-level union does.

    tags=['financial_transactions'] only, deliberately NOT also 'sales' --
    tagging both would make dbt select this node under *both*
    _build_dbt_assets_for_feed("financial_transactions") and
    _build_dbt_assets_for_feed("sales"), and Dagster rejects two different
    asset defs claiming the same AssetKey (confirmed for real: this broke
    a `just smoketest` run before this single-tag fix -- and the same
    duplicate-key problem independently applies to the generated serve
    views, see generate_serve_views.py's `owning_tag = feed_tags[0]`,
    which is why the single tag chosen here must be the alphabetically
    first depends_on_feeds member -- 'financial_transactions' < 'sales').
    Every generated per-feed job's AssetSelection is
    `.groups(feed).upstream()` (see scripts/generate_dagster_pipeline.py),
    which pulls stg_sales in as a real Dagster dependency whenever
    financial_transactions_job runs standalone -- so depends_on_feeds (the
    metadata concept, used for gating/updates_enabled sourcing) and this
    tag (which determines dbt-asset ownership) are allowed to diverge --
    this comment is that divergence's documented reason, not an oversight.

    Both sources are business-immutable (see stg_sales.sql/
    stg_financial_transactions.sql's own reasoning) -- Type 1, no deletion
    synthesis needed, same insert/update-split pattern as fct_sales.sql.
#}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with sales_activity as (

    select
        cast(sale_timestamp as date) as activity_date,
        'sales' as source_feed,
        invoice_id as source_id,
        branch as category,
        total as amount,
        false as is_deleted
    from {{ ref('stg_sales') }}

),

financial_activity as (

    select
        cast(posted_date as date) as activity_date,
        'financial_transactions' as source_feed,
        transaction_id as source_id,
        cost_center as category,
        debit_amount - credit_amount as amount,
        false as is_deleted
    from {{ ref('stg_financial_transactions') }}

),

base as (

    select * from sales_activity
    union all
    select * from financial_activity

),

hashed as (

    select
        *,
        {{ row_hash(['source_feed', 'source_id']) }} as _key_hash,
        {{ row_hash(['activity_date', 'category', 'amount', 'is_deleted']) }} as _attr_hash
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
