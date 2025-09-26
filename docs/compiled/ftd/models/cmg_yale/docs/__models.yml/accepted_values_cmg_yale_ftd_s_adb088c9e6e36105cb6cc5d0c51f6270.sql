
    
    

with all_values as (

    select
        subject_type as value_field,
        count(*) as n_records

    from "dbt"."main_cmg_yale_data"."cmg_yale_ftd_subject"
    group by subject_type

)

select *
from all_values
where value_field not in (
    'participant','non_participant','cell_line','animal_model','group','other'
)


