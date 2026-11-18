{#
    Layer -> ClickHouse database mapping.

    dbt's default appends the custom schema to the target schema. Here the
    generated name is `<target.schema>_<custom schema>`, which matches the
    `fineract_*` databases created in 01_databases.sql when target.schema
    is `fineract`. Check the target's schema before changing this macro: a
    different value writes to databases the init DDL never created.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
