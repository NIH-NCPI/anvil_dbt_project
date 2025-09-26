




    
    

select distinct
  sample_id::text as "Sample_id",
  processing::text as "processing"
from "dbt"."main_main"."fallback_table"
