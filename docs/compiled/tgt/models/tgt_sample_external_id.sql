




    
    

select distinct
  sample_id::text as "Sample_id",
  external_id::text as "external_id"
from "dbt"."main_main"."fallback_table"
