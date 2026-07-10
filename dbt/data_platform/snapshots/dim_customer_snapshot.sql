{% snapshot dim_customer_snapshot %}

{{
    config(
        target_schema='model',
        unique_key='_key_hash',
        strategy='check',
        check_cols=['_attr_hash'],
        snapshot_meta_column_names={
            "dbt_scd_id": "_scd_id",
            "dbt_valid_from": "_valid_from",
            "dbt_valid_to": "_valid_to",
            "dbt_updated_at": "_updated_at",
        },
        tags=['customers'],
    )
}}

{#
    Type 2 dimension (see Roadmap.md "Model Layer: SCD Design"). `_key_hash`
    is the business-key match column (dbt's `unique_key`); `_attr_hash`
    folds in tracked_columns (name, email) plus is_deleted -- a deletion
    flipping is_deleted true changes _attr_hash exactly like any other
    attribute update, so `check_cols=['_attr_hash']` closes the current
    version and opens a new one for either case with no special-casing.
    An unchanged _attr_hash (including an already-deleted row re-emitted
    every run by int_customers_with_deletes) produces zero new versions.
#}
with hashed as (

    select
        customer_id,
        name,
        email,
        updated_at,
        is_deleted,
        {{ row_hash(['customer_id']) }} as _key_hash,
        {{ row_hash(['name', 'email', 'is_deleted']) }} as _attr_hash
    from {{ ref('int_customers_with_deletes') }}

)

select * from hashed

{% endsnapshot %}
