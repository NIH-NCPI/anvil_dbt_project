




    
    

select distinct
  study_id::text as "Study_id",
  principal_investigator::text as "principal_investigator"
from "dbt"."main_main"."fallback_table"
