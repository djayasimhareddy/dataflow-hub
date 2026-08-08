-- Without this, dbt's default behavior would create Silver/Gold
-- tables in schemas named "public_silver" / "public_gold" (it
-- concatenates the custom schema onto the profile's default schema).
-- This override makes it just "silver" / "gold" instead, matching
-- the Bronze/Silver/Gold naming used everywhere else in this project.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
