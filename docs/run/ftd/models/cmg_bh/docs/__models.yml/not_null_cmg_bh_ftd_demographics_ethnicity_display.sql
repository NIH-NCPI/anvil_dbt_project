
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ethnicity_display
from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_demographics"
where ethnicity_display is null



  
  
      
    ) dbt_internal_test