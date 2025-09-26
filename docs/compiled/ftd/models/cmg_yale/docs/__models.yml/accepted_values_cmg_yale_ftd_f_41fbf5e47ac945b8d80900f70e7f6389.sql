
    
    

with all_values as (

    select
        family_type as value_field,
        count(*) as n_records

    from "dbt"."main_cmg_yale_data"."cmg_yale_ftd_family"
    group by family_type

)

select *
from all_values
where value_field not in (
    'control_only','duo','proband_only','trio','trio_plus','other'
)


