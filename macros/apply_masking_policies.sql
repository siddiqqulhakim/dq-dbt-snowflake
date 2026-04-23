{% macro apply_masking_policies() %}
  {% if this is none %}
    {{ return('') }}
  {% endif %}
  {% set alter_kw = 'VIEW' if this.identifier | upper | truncate(3, False, '') == 'BRZ' else 'TABLE' %}
  {% set registry = var('masking_policy_registry', {}) %}

  {% for col_name, col in model.columns.items() %}
    {% set policy_short = none %}

    {% set raw = col.masking_policy | string %}
    {% if raw and raw != 'None' and raw != '' %}
      {% set policy_short = raw %}
    {% endif %}

    {% if not policy_short and col.meta is mapping %}
      {% set policy_short = col.meta.get('masking_policy', none) %}
    {% endif %}

    {% if policy_short and policy_short in registry %}
      ALTER {{ alter_kw }} {{ this }} MODIFY COLUMN {{ col_name }} SET MASKING POLICY {{ registry[policy_short] }};
    {% endif %}
  {% endfor %}
{% endmacro %}
