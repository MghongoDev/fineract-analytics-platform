{#
    Layer -> ClickHouse database mapping.

    dbt's default appends the custom schema to the target schema
    (fineract_staging becomes fineract_fineract_staging). We want the
    layer databases created in 01_databases.sql to be used verbatim, so
    the custom schema wins outright.

    dev/ci/prod all resolve the same way; environment separation is done
    with a whole ClickHouse instance (or a CLICKHOUSE_DATABASE prefix),
    not with schema suffixes - suffix-based separation is how a CI run
    ends up writing into a production relation.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
