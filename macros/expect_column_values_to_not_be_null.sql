{% test expect_column_values_to_not_be_null(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is null

{% endtest %}
