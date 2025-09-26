




    
    

select distinct
  sample_id::text as "Sample_id",
  storage_method::text as "storage_method"
from "dbt"."main_main"."fallback_table"
