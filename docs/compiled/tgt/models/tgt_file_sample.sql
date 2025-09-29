




    
    

select distinct
  file_id::text as "File_id",
  sample_id::text as "sample_id"
from "dbt"."main_main"."fallback_table"
