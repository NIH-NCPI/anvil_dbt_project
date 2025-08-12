
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select data_access_type
from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_accesspolicy_data_access_type"
where data_access_type is null



  
  
      
    ) dbt_internal_test