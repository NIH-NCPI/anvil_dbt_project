
    
    

with all_values as (

    select
        availablity_status as value_field,
        count(*) as n_records

    from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_sample"
    group by availablity_status

)

select *
from all_values
where value_field not in (
    'available','unavailable'
)


