{% test expect_table_row_count_to_be_between(model, min_value=None, max_value=None) %}

select 1
from (
    select count(*) as row_count
    from {{ model }}
) counts
where 1=1
  {% if min_value is not none %}
    and row_count < {{ min_value }}
  {% endif %}
  {% if max_value is not none %}
    and row_count > {{ max_value }}
  {% endif %}

{% endtest %}
