{# Use the schema named in dbt_project.yml as-is ("marts"), not "<target schema>_marts". #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
