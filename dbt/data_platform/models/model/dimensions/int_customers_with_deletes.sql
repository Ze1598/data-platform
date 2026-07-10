{{ config(materialized='view', tags=['customers']) }}

{#
    Deletion synthesis for a deletions_enabled feed (see Roadmap.md
    "Deletion mechanism"). Compares stg_customers (every business key ever
    seen, cumulative -- staging never shrinks) against clean.customers
    (this run's true full snapshot -- clean is a fresh per-run load, not
    cumulative, see Roadmap.md "Layer Model"). A key in staging but missing
    from clean's current run is a deletion; its last-known attributes are
    carried forward from staging with is_deleted=true.

    Deliberately does NOT read dim_customer_snapshot's own current state to
    check "is this key already marked deleted" -- that would be a circular
    ref() (this model feeds the snapshot, not the other way around). Not
    needed anyway: a repeatedly-synthesized is_deleted=true row for an
    already-deleted key has the same _attr_hash every run (tracked_columns
    are frozen once deleted, and is_deleted stays true), so the snapshot's
    check_cols gate naturally produces zero new versions for it. Same
    idempotency mechanism already doing the work, not extra logic.
#}

with all_known_keys as (

    select customer_id, name, email, updated_at
    from {{ ref('stg_customers') }}

),

current_source_keys as (

    select customer_id from {{ source('clean', 'customers') }}

),

active as (

    select customer_id, name, email, updated_at, false as is_deleted
    from all_known_keys
    where customer_id in (select customer_id from current_source_keys)

),

deleted as (

    select customer_id, name, email, updated_at, true as is_deleted
    from all_known_keys
    where customer_id not in (select customer_id from current_source_keys)

)

select * from active
union all
select * from deleted
