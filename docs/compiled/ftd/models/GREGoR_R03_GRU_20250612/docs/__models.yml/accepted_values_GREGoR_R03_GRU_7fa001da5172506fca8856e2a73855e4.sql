
    
    

with all_values as (

    select
        sex as value_field,
        count(*) as n_records

    from "dbt"."main_GREGoR_R03_GRU_20250612_data"."GREGoR_R03_GRU_20250612_ftd_demographics"
    group by sex

)

select *
from all_values
where value_field not in (
    'female','male','unknown','intersex'
)


