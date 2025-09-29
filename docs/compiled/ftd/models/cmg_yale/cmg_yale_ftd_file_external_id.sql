

select 
  'fl' || '_' || md5('cmg_yale' || '|' || cast(coalesce(name, '') as text))::text as "file_id",
  name::text as "external_id"
from 
  "dbt"."main_main"."cmg_yale_stg_file_inventory"