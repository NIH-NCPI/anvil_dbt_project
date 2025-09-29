




    
    

select distinct
  disease_limitation::text as "disease_limitation",
  description::text as "description",
  website::text as "website",
  id::text as "id"
from "dbt"."main_main"."fallback_table"
