
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id
from "dbt"."main_cmg_bh_data"."cmg_bh_ftd_file"
where id is null



  
  
      
    ) dbt_internal_test