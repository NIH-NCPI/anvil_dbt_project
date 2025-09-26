




    
    

select distinct
  accesspolicy_id::text as "AccessPolicy_id",
  data_access_type::text as "data_access_type"
from "dbt"."main_main"."fallback_table"
