




    
    

select distinct
  data_source::text as "data_source",
  has_access_policy::text as "has_access_policy",
  id::text as "id"
from "dbt"."main_main"."fallback_table"
