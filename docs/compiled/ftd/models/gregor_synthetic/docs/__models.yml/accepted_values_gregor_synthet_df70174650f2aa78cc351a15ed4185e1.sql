
    
    

with all_values as (

    select
        date_of_birth_type as value_field,
        count(*) as n_records

    from (select * from "dbt"."main_gregor_synthetic_data"."gregor_synthetic_ftd_demographics" where date_of_birth_type is not null) dbt_subquery
    group by date_of_birth_type

)

select *
from all_values
where value_field not in (
    'exact','year_only','shifted','decade_only','other'
)


