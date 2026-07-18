{{ config(schema='serve', materialized='view', tags=['streaming', 'streaming_sales', 'streaming_inventory_events']) }}

{#
    TODO: describe this streaming view's real business logic here.
    Generated scaffold (scripts/generate_streaming_serve_scaffolds.py).
    Join to whichever model-layer dimension(s) make this stream useful
    (e.g. ref('sales_dim_branch')) -- no metadata describes which
    dimension or join key to use, same reasoning
    generate_model_scaffolds.py already gives for fact->dimension joins;
    that's real business logic.

    friendly_name (display label): inventory_events
    source table:                  streaming.inventory_events
#}

-- TODO: verify/adjust -- replace with the real business-logic select.
select * from {{ source('streaming', 'inventory_events') }}
