
with 
get_sids as (
  select crai as file, subject_id
  from "dbt"."main_main"."cmg_yale_stg_sample"
  where crai is not null
    union all 
  select cram as file, subject_id 
  from "dbt"."main_main"."cmg_yale_stg_sample"
  where cram is not null
)

select 
 distinct 
  'fl' || '_' || md5('cmg_yale' || '|' || cast(coalesce(name, '') as text))::text as "file_id",
  'sm' || '_' || md5('cmg_yale' || '|' || cast(coalesce(subject_id, '') as text))::text as "subject_id"
from 
  get_sids
  left join "dbt"."main_main"."cmg_yale_stg_file_inventory"
    on file = file_ref