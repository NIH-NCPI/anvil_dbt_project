




    
    

select distinct
  datasource_id::text as "DataSource_id",
  external_id::text as "external_id"
from "dbt"."main_main"."fallback_table"
