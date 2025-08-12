
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select relationship_code
from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_familyrelationship"
where relationship_code is null



  
  
      
    ) dbt_internal_test