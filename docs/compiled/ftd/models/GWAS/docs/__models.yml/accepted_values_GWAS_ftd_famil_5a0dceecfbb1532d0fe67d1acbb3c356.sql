
    
    

with all_values as (

    select
        consanguinity as value_field,
        count(*) as n_records

    from "dbt"."main_GWAS_data"."GWAS_ftd_family"
    group by consanguinity

)

select *
from all_values
where value_field not in (
    'not_suspected','suspected','known_present','unknown'
)


