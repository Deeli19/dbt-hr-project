{% macro parse_date(column_name, default_format='YYYY-MM-DD') %}

coalesce(
    try_to_date({{ column_name }}, '{{ default_format }}'),
    try_to_date({{ column_name }}, 'DD-MM-YYYY'),
    try_to_date({{ column_name }}, 'DD-MON-YY'),
    try_to_date({{ column_name }}, 'MM/DD/YYYY')
)

{% endmacro %}