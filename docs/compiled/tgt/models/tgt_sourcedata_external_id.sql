




    
    

select distinct
  sourcedata_id::text as "SourceData_id",
  external_id::text as "external_id"
from "dbt"."main_main"."fallback_table"
