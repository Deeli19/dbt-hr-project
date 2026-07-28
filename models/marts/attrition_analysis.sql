with inactive_employee as (

    select * from {{ ref('fct_employee_monthly_archive') }}
    where is_active_employee = false

),

terminated_employees as (

    select *,
        employment_exit_date - employment_start_date as tenure_days

    from inactive_employee

    where is_terminated_employee = 1

),

final as (

    select

        division,
        department_type,

        count(*) as terminated_employee_count,

        round(avg(tenure_days), 0) as avg_tenure_before_exit

    from terminated_employees

    group by 1, 2

)

select * from final