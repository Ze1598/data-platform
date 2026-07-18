{{ config(schema='serve', materialized='view', tags=['sales', 'streaming']) }}

{#
    Real-time sales_events (Kafka -> Flink -> Iceberg, streaming/ module)
    joined to the already-persisted model-layer dimension. No new
    query-engine integration needed for this join -- both sides are plain
    Iceberg tables in the same Polaris-cataloged warehouse (Roadmap.md
    Phase 11's stated architectural property, confirmed live: this table
    was created and is being continuously written by Flink, a completely
    separate engine from Trino, and is queryable here with zero Trino/
    Polaris config changes). Hand-authored, not generated -- this table
    has no lakehouse_models row and never will under the current
    first-pass design (streaming stays serve-only for now, see
    Roadmap.md/Backlog.md), so it's deliberately outside serve/generated/
    and untouched by generate_serve_views.py's regenerate-and-wipe.
#}

select
    e.event_id,
    e.event_type,
    e.product_line,
    e.amount,
    e.event_timestamp,
    b.branch,
    b.city
from {{ source('streaming', 'sales_events') }} e
left join {{ ref('sales_dim_branch') }} b
    on e.branch = b.branch
   and b.is_deleted = false
