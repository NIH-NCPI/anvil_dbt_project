




    
    

select distinct
  subjectassertion_id::text as "SubjectAssertion_id",
  source_data_id::text as "source_data_id"
from "dbt"."main_main"."fallback_table"
