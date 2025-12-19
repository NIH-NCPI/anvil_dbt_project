
    
    

with all_values as (

    select
        consanguinity as value_field,
        count(*) as n_records

    from (select * from "dbt"."main_cser_data"."cser_ftd_family" where consanguinity is not null) dbt_subquery
    group by consanguinity

)

select *
from all_values
where value_field not in (
    'not_suspected','suspected','known_present','unknown'
)


