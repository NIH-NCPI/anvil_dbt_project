




    
    

select distinct
  code::text as "code",
  display::text as "display",
  value_code::text as "value_code",
  value_display::text as "value_display",
  id::text as "id"
from "dbt"."main_main"."fallback_table"
