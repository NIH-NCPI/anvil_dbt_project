
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select study_id
from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_study_external_id"
where study_id is null



  
  
      
    ) dbt_internal_test