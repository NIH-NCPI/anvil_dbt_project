
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select familymember_id
from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_familymember_external_id"
where familymember_id is null



  
  
      
    ) dbt_internal_test