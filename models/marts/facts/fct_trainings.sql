with training as (

    select * from {{ ref('stg_hr__trainings') }}

),

final as (

    select

        employee_id,
        training_date,

        training_name,
        training_type,
        trainer,
        location,

        training_days,
        training_cost,

        training_outcome

    from training

)

select * from final