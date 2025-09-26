




    
    

select distinct
  family_member::text as "family_member",
  other_family_member::text as "other_family_member",
  relationship_code::text as "relationship_code",
  has_access_policy::text as "has_access_policy",
  id::text as "id"
from "dbt"."main_main"."fallback_table"
