
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select description
from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_accesspolicy"
where description is null



  
  
      
    ) dbt_internal_test