




    
    

select distinct
  file_id::text as "File_id",
  external_id::text as "external_id"
from "dbt"."main_main"."fallback_table"
