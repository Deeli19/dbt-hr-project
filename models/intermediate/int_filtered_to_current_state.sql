-- Grain: 1 row per source employee record

with employee as (

    select * from {{ ref('int_canonical_identity_resolved') }}

),
 
employee_current_state as (

    select * from employee
    where dbt_valid_to is null


)

select * from employee_current_state