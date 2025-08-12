
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select family_relationships_id
from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_family_family_relationships"
where family_relationships_id is null



  
  
      
    ) dbt_internal_test