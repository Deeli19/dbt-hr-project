with employee as (

    select *
    from {{ ref('stg_hr__employee') }}

),

employee_identity as (

    select *
    from {{ ref('int_employee_identity') }}
),

final as (

    select
        -- primary key
        employee_id,
        --canonical_employee_id

        -- identity
        concat(first_name, ' ', last_name) as full_name,
        email,

        -- demographics
        date_of_birth,
        gender_code,

        -- employment info
        employee_status_group,
        start_date,
        exit_date,
        is_current_employee,
        is_terminated,

        -- tenure (simple version)
        --count(*) over (partition by canonical_employee_id) as employment_record_count,
        --case when employment_record_count > 1 then 1 else 0 end as is_rehire
        datediff('day', start_date, coalesce(exit_date, current_date)) as tenure_days

    from employee e
    join employee_identity ei
        on e.employee_id = ei.employee_id

)

select * from final