{# Mandala URN parsing helpers.

   URN format: urn:mandala:<entity>:<scope>:<id>
   We split into three parts and surface them as columns in staging.
#}

{% macro mandala_urn_entity(urn_col) -%}
    split_part({{ urn_col }}, ':', 3)
{%- endmacro %}

{% macro mandala_urn_scope(urn_col) -%}
    split_part({{ urn_col }}, ':', 4)
{%- endmacro %}

{% macro mandala_urn_id(urn_col) -%}
    {{ adapter.dispatch('mandala_urn_id', 'mandala')(urn_col) }}
{%- endmacro %}

{% macro default__mandala_urn_id(urn_col) -%}
    substring({{ urn_col }} from position(':' in substring({{ urn_col }} from position(':' in substring({{ urn_col }} from position(':' in {{ urn_col }})+1)+1)+1)+1)
{%- endmacro %}

{% macro snowflake__mandala_urn_id(urn_col) -%}
    SPLIT_PART({{ urn_col }}, ':', 5)
{%- endmacro %}

{% macro bigquery__mandala_urn_id(urn_col) -%}
    SPLIT({{ urn_col }}, ':')[SAFE_OFFSET(4)]
{%- endmacro %}

{% macro redshift__mandala_urn_id(urn_col) -%}
    SPLIT_PART({{ urn_col }}, ':', 5)
{%- endmacro %}

{% macro duckdb__mandala_urn_id(urn_col) -%}
    string_split({{ urn_col }}, ':')[5]
{%- endmacro %}

{% macro postgres__mandala_urn_id(urn_col) -%}
    SPLIT_PART({{ urn_col }}, ':', 5)
{%- endmacro %}
