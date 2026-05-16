select
    employee_id
from {{ ref('dim_employee_current') }}
where is_active_employee = true
and employment_exit_date is not null