{% macro generate_schema_name(custom_schema_name, node) %}

    {%- set base_schema = target.schema -%}
    {%- set parts = node.fqn -%}

    {# correct for your structure: models/hr/staging/model.sql #}
    {%- set domain = parts[1] if parts | length > 1 else 'shared' -%}
    {%- set layer  = parts[2] if parts | length > 2 else '' -%}

    {%- set layers = ['staging', 'intermediate', 'marts', 'core', 'analytics'] -%}

    {%- if target.name == 'dev' -%}

        {%- if layer in layers -%}
            {{ base_schema ~ '_' ~ domain ~ '_' ~ layer }}
        {%- else -%}
            {{ base_schema ~ '_' ~ domain }}
        {%- endif -%}

    {%- elif target.name == 'prod' -%}

        {%- if custom_schema_name is not none -%}
            {{ custom_schema_name }}
        {%- elif layer in layers -%}
            {{ domain ~ '_' ~ layer }}
        {%- else -%}
            {{ domain }}
        {%- endif -%}

    {%- else -%}

        {{ base_schema }}

    {%- endif -%}

{% endmacro %}