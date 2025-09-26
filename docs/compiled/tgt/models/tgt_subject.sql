




    
    

select distinct
  subject_type::text as "subject_type",
  organism_type::text as "organism_type",
  has_access_policy::text as "has_access_policy",
  id::text as "id",
  has_demographics_id::text as "has_demographics_id"
from "dbt"."main_main"."fallback_table"
