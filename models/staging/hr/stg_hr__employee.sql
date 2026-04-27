with source as (

    select * 
    from {{ source('hr', 'employee') }}

),

renamed as (

    select
        -- primary key
        emp_id as employee_id,

        -- text fields
        lower(trim(first_name)) as first_name,
        lower(trim(last_name)) as last_name,

        -- emails
        lower(trim(ad_email)) as email,

        -- dates
        {{ parse_date('start_date', 'DD-MON-YY') }} as start_date,
        {{ parse_date('exit_date', 'DD-MON-YY') }} as exit_date,
        {{ parse_date('dob', 'DD-MON-YY') }} as date_of_birth,

        -- categorical
        employee_status,

        -- metadata
        current_timestamp as _loaded_at

    from source

)

select * from renamed