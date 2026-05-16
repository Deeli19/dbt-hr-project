{% macro calculate_tenure_days(start_date, end_date) %}

datediff(
    'day',
    {{ start_date }},
    coalesce({{ end_date }}, current_date)
)

{% endmacro %}