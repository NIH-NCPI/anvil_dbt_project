




    
    

select distinct
  sourcedata_id::text as "SourceData_id",
  query_parameter::text as "query_parameter"
from "dbt"."main_main"."fallback_table"
