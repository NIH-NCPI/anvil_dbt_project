




    
    

select distinct
  subject_id::text as "Subject_id",
  external_id::text as "external_id"
from "dbt"."main_main"."fallback_table"
