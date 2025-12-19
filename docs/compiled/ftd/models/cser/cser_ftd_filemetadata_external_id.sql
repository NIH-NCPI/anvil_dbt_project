

with unioned_names as (
        select
            distinct 
            file_id as "file_id"
        from "dbt"."main_main"."cser_stg_file_inventory"
    
        union all
   
        select
            distinct 
            sequencing_id as "file_id"
        from "dbt"."main_main"."cser_stg_sequencing"
)

select
  'fd' || '_' || md5('cser' || '|' || cast(coalesce(file_id, '') as text))::text as "filemetadata_id",
  file_id::text as "external_id"
from "dbt"."main_main"."cser_stg_file_inventory" as file_inventory