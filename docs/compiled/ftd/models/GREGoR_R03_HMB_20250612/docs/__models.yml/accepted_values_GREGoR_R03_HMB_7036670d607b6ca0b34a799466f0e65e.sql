
    
    

with all_values as (

    select
        access_policy_code as value_field,
        count(*) as n_records

    from "dbt"."main_GREGoR_R03_HMB_20250612_data"."GREGoR_R03_HMB_20250612_ftd_accesspolicy_access_policy_code"
    group by access_policy_code

)

select *
from all_values
where value_field not in (
    'gru','hmb','ds','irb','pub','col','npu','mds','gso','gsr'
)


