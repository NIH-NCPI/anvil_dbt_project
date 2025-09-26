




    
    

select distinct
  biospecimencollection_id::text as "BiospecimenCollection_id",
  external_id::text as "external_id"
from "dbt"."main_main"."fallback_table"
