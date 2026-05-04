{{ config(
    materialized='incremental',
    unique_key='applicant_id'
) }}

with recruitment as (

    select * from {{ ref('stg_hr__recruitment') }}

    {% if is_incremental() %}

        where application_date > (
            select max(application_date)
            from {{ this }}
        )

    {% endif %}

),

final as (

    select

        applicant_id,
        application_date,
        application_status,
        job_title,
        years_of_experience,
        desired_salary,

        case
            when application_status = 'Offered' then 1
            else 0
        end as hired_flag

    from recruitment

)

select * from final