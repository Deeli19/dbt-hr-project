{{ config(materialized='table') }}

with employee as (

    select *
    from {{ ref('stg_hr__employee') }}

),

final as (

    select
        -- primary key
        employee_id,

        -- names
        first_name,
        last_name,
        concat(first_name, ' ', last_name) as full_name,

        -- email
        email,

        -- demographics
        date_of_birth,
        gender_code,

        -- employment info
        employee_status,

        case
            when lower(employee_status) like '%terminate%' then 'terminated'
            when lower(employee_status) like '%active%' then 'active'
            when lower(employee_status) like '%leave%' then 'leave'
            else 'other'
        end as employee_status_group,

        start_date,
        exit_date,

        -- tenure (simple version)
        datediff('day', start_date, coalesce(exit_date, current_date)) as tenure_days,

        -- current employee flag
        case
            when exit_date is null then true
            when employee_status = 'Future Start' then false
            else false
        end as is_current_employee

    from employee

)

select * from final