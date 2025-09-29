
    
    

with all_values as (

    select
        date_of_birth_type as value_field,
        count(*) as n_records

    from "dbt"."main_PGRNseq_data"."PGRNseq_ftd_demographics"
    group by date_of_birth_type

)

select *
from all_values
where value_field not in (
    'exact','year_only','shifted','decade_only','other'
)


