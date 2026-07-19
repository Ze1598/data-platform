{{ config(schema='serve', materialized='view', tags=['streaming', 'streaming_sales', 'streaming_inventory_events']) }}

{#
    Real-time inventory_events (Kafka -> Flink -> Iceberg, streaming/
    module) joined to the already-persisted sales_dim_branch model table --
    same join shape as sales_events.sql (branch -> city enrichment), the
    proven pattern for this domain. Written as the worked example for
    Walkthrough_New_Streaming_Source.md; see that file for the full,
    reproducible onboarding sequence this join is the last step of.
#}

select
    e.event_id,
    e.sku,
    e.branch,
    b.city,
    e.quantity_change,
    e.event_timestamp
from {{ source('streaming', 'inventory_events') }} e
left join {{ ref('sales_dim_branch') }} b
    on e.branch = b.branch
   and b.is_deleted = false
