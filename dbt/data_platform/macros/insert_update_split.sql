{#
    Explicit insert/update split, not a MERGE (see stg_customers.sql for
    the full reasoning behind why this project avoids MERGE for this
    pattern). Every row in `source` is pre-classified by the calling
    model's own query into a transient `_change_type` column ('insert' or
    'update') -- computed on the is_incremental() branch only, so it never
    leaks into a freshly-created table's persisted schema on the very
    first run (a first run never calls this macro at all; the
    incremental materialization only invokes the configured strategy once
    the target table already exists).

    Applied as two separate, targeted statements: DELETE the old version
    of every row classified 'update', then INSERT everything classified
    'insert' or 'update' in one pass (the now-deleted rows' new values
    land back in via this same INSERT). This achieves "update" semantics
    using only DELETE + INSERT, both of which Trino's Iceberg connector
    handles natively and efficiently, without Trino ever having to plan a
    MERGE's matched/unmatched branching in one query.

    Overrides dbt-trino's shipped `trino__get_delete_insert_merge_sql`
    (which does an unconditional "delete every matching key, then insert
    everything from source" -- functionally close, but has no concept of
    `_change_type`, so it can't distinguish insert from update, and always
    runs the delete even for a feed where updates are disabled). Reuses
    the `delete+insert` incremental_strategy name -- already in the
    adapter's whitelist -- rather than inventing a new strategy name,
    which the Trino adapter's Python class would reject outright
    (`TrinoAdapter.valid_incremental_strategies()` is a hardcoded list,
    not something a project can extend via macros).
#}
{% macro trino__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}

    {% if unique_key %}
        delete from {{ target }}
        where {{ unique_key }} in (
            select {{ unique_key }} from {{ source }} where _change_type = 'update'
        )
        {%- if predicates %}
            {% for predicate in predicates %}
                and {{ predicate }}
            {% endfor %}
        {%- endif %};
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
        {% if unique_key %}
        where _change_type in ('insert', 'update')
        {% endif %}
    )
{%- endmacro %}

{#
    The other half of the insert/update split: the single join against
    the target that both classifies each row ('insert'/'update' via
    _change_type, consumed by trino__get_delete_insert_merge_sql above)
    and *is* the change-detection step -- not a pre-filter ahead of a
    second join inside a generated MERGE, which is what every one of the
    6 models using this replaced (see Learnings.md, "Explicit
    insert/update split instead of MERGE"). Assumes the standard
    _key_hash/_attr_hash pair every model in this project computes via
    row_hash() -- not a generic arbitrary-key join, deliberately, since
    every caller already has both hashes by the time this runs.

    `source_relation` is the upstream CTE name providing this run's rows
    (already hashed); `updates_enabled` (from data_feed.updates_enabled or
    model_feed.updates_enabled, threaded in via the updates_enabled_by_model
    dbt var -- see dbt_assets.py) gates whether the attribute-hash branch
    is even compiled in: false means this feed/model is insert-only, an
    existing business key is never re-evaluated for attribute changes.
#}
{% macro classify_changes(source_relation, updates_enabled) %}
    select
        {{ source_relation }}.*,
        case when target._key_hash is null then 'insert' else 'update' end as _change_type
    from {{ source_relation }}
    left join {{ this }} as target
        on {{ source_relation }}._key_hash = target._key_hash
    where target._key_hash is null                                   {# new business key #}
       {% if updates_enabled %}
       or target._attr_hash != {{ source_relation }}._attr_hash      {# changed attributes #}
       {% endif %}
{% endmacro %}
