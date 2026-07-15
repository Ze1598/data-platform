{#
    dbt's default current_timestamp renders TIMESTAMP(3) (millisecond
    precision); Iceberg only supports microsecond precision, so Trino can't
    write timestamp(3) into an Iceberg table. Override to precision 6.
    See Roadmap.md "Iceberg-specific caveats to carry into implementation".
#}
{% macro trino__current_timestamp() %}
current_timestamp(6)
{%- endmacro %}
