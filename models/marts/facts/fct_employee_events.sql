-- Grain: 1 row per employee attribute change event. 
-- Initial events are ignored since they represent the initial state of the employee rather than a change event.

{% set tracked_attributes = [
    {
        "event_type": "employee_status_change",
        "attribute": "employee_status",
        "previous": "previous_employee_status",
        "current": "employee_status"
    },
    {
        "event_type": "title_change",
        "attribute": "title",
        "previous": "previous_title",
        "current": "title"
    },
    {
        "event_type": "division_transfer",
        "attribute": "division",
        "previous": "previous_division",
        "current": "division"
    },
    {
        "event_type": "department_type_change",
        "attribute": "department_type",
        "previous": "previous_department_type",
        "current": "department_type"
    },
    {
        "event_type": "supervisor_change",
        "attribute": "supervisor",
        "previous": "previous_supervisor",
        "current": "supervisor"
    }
] %}

with employee_changes as (

    select *

    from {{ ref('int_state_history_preserved') }}

),

initial_hire_events as (

    select

        {{ dbt_utils.generate_surrogate_key([
            'employee_id',
            'effective_start_date',
            "'hire_event'"
        ]) }} as employee_event_id,

        employee_id,
        employee_history_id,

        effective_start_date as event_timestamp,
        cast(effective_start_date as date) as event_date,

        'hire_event' as event_type,

        null as previous_value,
        employee_status as new_value,

        'employee_status' as changed_attribute

    from employee_changes

    where previous_employee_status is null

),

employee_status_events as (

    select

        {{ dbt_utils.generate_surrogate_key([
            'employee_id',
            'effective_start_date',
            "'" ~ attr.event_type ~ "'"
        ]) }} as employee_event_id,

        employee_id,
        employee_history_id,

        effective_start_date as event_timestamp,
        cast(effective_start_date as date) as event_date,

        '{{ attr.event_type }}' as event_type,

        {{ attr.previous }} as previous_value,
        {{ attr.current }} as new_value,

        '{{ attr.attribute }}' as changed_attribute

    from employee_changes

    where coalesce({{ attr.previous }}, 'unknown')
        != coalesce({{ attr.current }}, 'unknown')

    {% if not loop.last %}
    union all
    {% endif %}

    {% endfor %}

)

select * from final