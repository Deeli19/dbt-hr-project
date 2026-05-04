with employee as (

    select * from {{ ref('dim_employee') }}

),

terminated_employees as (

    select *

    from employee

    where is_terminated = 1

),

final as (

    select

        division,
        department_type,
        termination_type,

        count(*) as terminated_employee_count,

        avg(tenure_years) as avg_tenure_before_exit

    from terminated_employees

    group by 1, 2, 3

)

select *

from final