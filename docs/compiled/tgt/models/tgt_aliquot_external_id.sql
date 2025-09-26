




    
    

select distinct
  aliquot_id::text as "Aliquot_id",
  external_id::text as "external_id"
from "dbt"."main_main"."fallback_table"
