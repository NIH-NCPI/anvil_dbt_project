
    
    

with all_values as (

    select
        availablity_status as value_field,
        count(*) as n_records

    from (select * from "dbt"."main_PGRNseq_data"."PGRNseq_ftd_sample" where availablity_status IS NOT NULL) dbt_subquery
    group by availablity_status

)

select *
from all_values
where value_field not in (
    'available','unavailable'
)


