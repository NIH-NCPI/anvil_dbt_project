
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        access_policy_code as value_field,
        count(*) as n_records

    from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_accesspolicy_access_policy_code"
    group by access_policy_code

)

select *
from all_values
where value_field not in (
    'gru','hmb','ds','irb','pub','col','npu','mds','gso','gsr'
)



  
  
      
    ) dbt_internal_test