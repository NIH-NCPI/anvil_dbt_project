
    
    

with all_values as (

    select
        availablity_status as value_field,
        count(*) as n_records

    from (select * from "dbt"."main_cser_data"."cser_ftd_sample" where availablity_status is not null) dbt_subquery
    group by availablity_status

)

select *
from all_values
where value_field not in (
    'available','unavailable'
)


