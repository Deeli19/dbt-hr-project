{% macro detect_attribute_change(attributes) %}

{% for attribute, label in attributes %}

when coalesce(previous_{{ attribute }}, 'unknown')
    != coalesce({{ attribute }}, 'unknown')
then '{{ label }}'

{% endfor %}

{% endmacro %}