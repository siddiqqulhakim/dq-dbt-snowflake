{% macro apply_snowflake_tags() %}
  {% if this is none %}
    {{ return('') }}
  {% endif %}
  {% set parts = this.identifier.lower().split('__') %}
  {% if parts | length >= 3 %}
    {% set layer   = parts[0] %}
    {% set domain  = parts[1] %}
    {% set context = parts[2:] | join('__') %}
    ALTER TABLE {{ this }} SET TAG DEMO_ENTERPRISE_DBT_DB.PUBLIC.dbt_layer   = '{{ layer }}';
    ALTER TABLE {{ this }} SET TAG DEMO_ENTERPRISE_DBT_DB.PUBLIC.dbt_domain  = '{{ domain }}';
    ALTER TABLE {{ this }} SET TAG DEMO_ENTERPRISE_DBT_DB.PUBLIC.dbt_context = '{{ context }}';
  {% endif %}
{% endmacro %}
