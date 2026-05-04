{% snapshot snap_employee %}

{{
    config(
        target_database='analytics',
        target_schema='snapshots',
        unique_key='employee_id',

        strategy='check',

        check_cols=[
            'employee_status',
            'title',
            'division',
            'department_type',
            'supervisor'
        ]
    )
}}

select *

from {{ ref('stg_hr__employee') }}

{% endsnapshot %}