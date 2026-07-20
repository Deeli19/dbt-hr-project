-- Grain: 1 row per source employee record

-- Purpose:
-- Enrich employee records with canonical identity attributes

with employee as (

    select *

    from {{ ref('employee_snapshot') }}

),

identity_resolution as (

    select 
        *,
        min(employee_id) over (
            partition by lower(email)
        ) as canonical_employee_id

    from employee

),

identify_multiple_employment_periods as (

    select
        *,
        -- counts number of employment records tied to the same canonical employee identity
        count(*) over (
            partition by canonical_employee_id
        ) as total_employment_periods
        
    from identity_resolution

),

create_rehire_flags as (

    select
        *,
        -- heuristic rehire flag. Assumes multiple employee records means rehire. Often true but not always.
        case
            when total_employment_periods > 1 then true
            else false
        end as is_rehire

    from identify_multiple_employment_periods

)

select * from create_rehire_flags