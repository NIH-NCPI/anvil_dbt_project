




    
    

select distinct
  study_id::text as "Study_id",
  funding_source::text as "funding_source"
from "dbt"."main_main"."fallback_table"
