{% test expect_column_values_to_be_in_set(model, column_name, value_set) %}

select *
from {{ model }}
where {{ column_name }} not in (
    {% for value in value_set %}
        '{{ value }}'{% if not loop.last %}, {% endif %}
    {% endfor %}
)
and {{ column_name }} is not null

{% endtest %}
