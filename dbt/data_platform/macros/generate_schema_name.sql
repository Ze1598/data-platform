{#
    dbt's default generate_schema_name macro concatenates a model's custom
    `schema` config onto the target's default schema (e.g. `schema='model'`
    would resolve to `staging_model`, since profiles.yml's target schema is
    `staging`) -- not what this project wants: clean, standalone `clean`/
    `staging`/`model` namespaces, matching the Layer Model (Roadmap.md) and
    what's pre-created in polaris_client/bootstrap.py's REQUIRED_NAMESPACES.
    Override to use the custom schema name verbatim, dbt's own documented
    pattern for this. Only affects models that set an explicit `schema`
    config (i.e. the model layer) -- staging models don't set one, so they
    still resolve to the target's default schema as before.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
