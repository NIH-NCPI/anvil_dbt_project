

select distinct
  'fl' || '_' || md5('cser' || '|' || cast(coalesce(file_id, '') as text))::text as "file_id",
  file_id::text as "external_id"
from "dbt"."main_main"."cser_stg_file_inventory" as file_inventory