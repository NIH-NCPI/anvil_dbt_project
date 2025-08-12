
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_data_id
from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_demographics_source_data"
where source_data_id is null



  
  
      
    ) dbt_internal_test