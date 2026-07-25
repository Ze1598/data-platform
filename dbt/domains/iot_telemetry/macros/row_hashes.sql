{#
    Shared hash-based key/non-key pattern (see Roadmap.md "Model Layer: SCD
    Design"). One macro, reused everywhere a merge happens (clean->staging,
    Type 1 dimensions, Type 2 snapshot check_cols, mutable facts) — callers
    just pass the column list, no per-table hashing logic.

    NULL-safe: a NULL column value is coalesced to a sentinel distinct from
    any real string, so NULL and empty-string don't collide.
#}
{% macro row_hash(columns) %}
to_hex(sha512(to_utf8(
    {%- for col in columns %}
    coalesce(cast({{ col }} as varchar), '~NULL~'){% if not loop.last %} || '|~|' || {% endif %}
    {%- endfor %}
)))
{%- endmacro %}
