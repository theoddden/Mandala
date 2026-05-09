{# Resolve the configured source location.

   Consumers override the location via vars in their dbt_project.yml; this
   macro returns the right relation regardless of whether they set
   ``raw_database`` (Snowflake/BigQuery) or just ``raw_schema``.
#}

{% macro mandala_raw_relation() -%}
    {%- set db = var('mandala', {}).get('raw_database') -%}
    {%- set schema = var('mandala', {}).get('raw_schema', 'raw') -%}
    {%- set table = var('mandala', {}).get('raw_table', 'raw_mandala_events') -%}
    {%- if db -%}
        {{ api.Relation.create(database=db, schema=schema, identifier=table) }}
    {%- else -%}
        {{ api.Relation.create(schema=schema, identifier=table) }}
    {%- endif -%}
{%- endmacro %}
