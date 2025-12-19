
    
    

with all_values as (

    select
        family_type as value_field,
        count(*) as n_records

    from (select * from "dbt"."main_cser_data"."cser_ftd_family" where family_type is not null) dbt_subquery
    group by family_type

)

select *
from all_values
where value_field not in (
    'control_only','duo','proband_only','trio','trio_plus','other'
)


