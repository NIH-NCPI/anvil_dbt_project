
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select has_access_policy
from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_sourcedata"
where has_access_policy is null



  
  
      
    ) dbt_internal_test