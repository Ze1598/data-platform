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

    Lives here as the CANONICAL source, copied verbatim into every domain's
    own macros/ directory by scripts/generate_domain_projects.py -- not
    installed as a dbt package dependency. Confirmed live, the hard way: no
    macro in this file -- not just this one, every macro in this directory,
    including plain ones like row_hash -- reliably resolves for a domain's
    own model nodes when dbt/_shared is installed via `dependencies.yml`'s
    local-path package mechanism and looked up through the normal import
    path. It works on a project's very first `dbt build` (fresh manifest,
    nothing cached) but silently breaks on every subsequent build in the
    same environment ('<macro> is undefined', even with --full-refresh and
    a fully wiped target/ dir) -- confirmed by dropping the physical table
    and rebuilding from cold, still broken; confirmed fixed immediately by
    copying this project's macros directly into the domain's own macros/
    directory instead of relying on the installed package. Root cause not
    further isolated (not worth chasing further for a project this size);
    physical duplication per domain is the practical, verified fix.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
