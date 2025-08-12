
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select other_family_member
from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_familyrelationship"
where other_family_member is null



  
  
      
    ) dbt_internal_test