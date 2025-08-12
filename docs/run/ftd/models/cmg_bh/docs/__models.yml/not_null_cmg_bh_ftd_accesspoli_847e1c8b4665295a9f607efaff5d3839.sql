
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select accesspolicy_id
from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_accesspolicy_access_policy_code"
where accesspolicy_id is null



  
  
      
    ) dbt_internal_test