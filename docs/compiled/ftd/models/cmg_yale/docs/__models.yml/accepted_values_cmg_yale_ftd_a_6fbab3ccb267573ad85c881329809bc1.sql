
    
    

with all_values as (

    select
        availablity_status as value_field,
        count(*) as n_records

    from "dbt"."main_cmg_yale_data"."cmg_yale_ftd_aliquot"
    group by availablity_status

)

select *
from all_values
where value_field not in (
    'available','unavailable'
)


