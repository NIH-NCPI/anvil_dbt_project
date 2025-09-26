




    
    

select distinct
  parent_study_id::text as "parent_study_id",
  study_title::text as "study_title",
  id::text as "id"
from "dbt"."main_main"."fallback_table"
