{# Cross-warehouse JSON helpers.

   Mandala stores the CloudEvents ``data`` payload as raw JSON in a single
   warehouse column whose type varies by adapter (VARIANT in Snowflake,
   JSON in BigQuery/Postgres, SUPER in Redshift). These macros normalize
   access so models stay portable.
#}

{% macro mandala_json_get(column, path) -%}
    {{ adapter.dispatch('mandala_json_get', 'mandala')(column, path) }}
{%- endmacro %}

{% macro default__mandala_json_get(column, path) -%}
    {{ column }} ->> '{{ path }}'
{%- endmacro %}

{% macro postgres__mandala_json_get(column, path) -%}
    {{ column }} ->> '{{ path }}'
{%- endmacro %}

{% macro snowflake__mandala_json_get(column, path) -%}
    {{ column }}:{{ path }}::string
{%- endmacro %}

{% macro bigquery__mandala_json_get(column, path) -%}
    JSON_VALUE({{ column }}, '$.{{ path }}')
{%- endmacro %}

{% macro redshift__mandala_json_get(column, path) -%}
    json_extract_path_text({{ column }}, '{{ path }}')
{%- endmacro %}

{% macro databricks__mandala_json_get(column, path) -%}
    {{ column }}:{{ path }}::string
{%- endmacro %}

{% macro duckdb__mandala_json_get(column, path) -%}
    json_extract_string({{ column }}, '$.{{ path }}')
{%- endmacro %}


{# Numeric variant of the above — casts to double / numeric. #}

{% macro mandala_json_get_number(column, path) -%}
    {{ adapter.dispatch('mandala_json_get_number', 'mandala')(column, path) }}
{%- endmacro %}

{% macro default__mandala_json_get_number(column, path) -%}
    ({{ column }} ->> '{{ path }}')::double precision
{%- endmacro %}

{% macro snowflake__mandala_json_get_number(column, path) -%}
    {{ column }}:{{ path }}::float
{%- endmacro %}

{% macro bigquery__mandala_json_get_number(column, path) -%}
    SAFE_CAST(JSON_VALUE({{ column }}, '$.{{ path }}') AS FLOAT64)
{%- endmacro %}

{% macro redshift__mandala_json_get_number(column, path) -%}
    json_extract_path_text({{ column }}, '{{ path }}')::double precision
{%- endmacro %}

{% macro duckdb__mandala_json_get_number(column, path) -%}
    CAST(json_extract_string({{ column }}, '$.{{ path }}') AS DOUBLE)
{%- endmacro %}


{# Timestamp variant. #}

{% macro mandala_json_get_timestamp(column, path) -%}
    {{ adapter.dispatch('mandala_json_get_timestamp', 'mandala')(column, path) }}
{%- endmacro %}

{% macro default__mandala_json_get_timestamp(column, path) -%}
    ({{ column }} ->> '{{ path }}')::timestamp
{%- endmacro %}

{% macro snowflake__mandala_json_get_timestamp(column, path) -%}
    TRY_TO_TIMESTAMP_TZ({{ column }}:{{ path }}::string)
{%- endmacro %}

{% macro bigquery__mandala_json_get_timestamp(column, path) -%}
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_VALUE({{ column }}, '$.{{ path }}'))
{%- endmacro %}

{% macro redshift__mandala_json_get_timestamp(column, path) -%}
    json_extract_path_text({{ column }}, '{{ path }}')::timestamp
{%- endmacro %}

{% macro duckdb__mandala_json_get_timestamp(column, path) -%}
    CAST(json_extract_string({{ column }}, '$.{{ path }}') AS TIMESTAMP)
{%- endmacro %}
