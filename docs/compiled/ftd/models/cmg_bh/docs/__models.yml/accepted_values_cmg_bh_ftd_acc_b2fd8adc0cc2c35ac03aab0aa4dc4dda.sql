
    
    

with all_values as (

    select
        data_access_type as value_field,
        count(*) as n_records

    from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_accesspolicy_data_access_type"
    group by data_access_type

)

select *
from all_values
where value_field not in (
    'open','registered','controlled','gsr_restricted','gsr_allowed'
)


