

with 
get_sm_files as (
  select crai as file, sample_id 
  from "dbt"."main_main"."cmg_yale_stg_sample"
  where crai is not null
    union all 
  select cram as file, sample_id 
  from "dbt"."main_main"."cmg_yale_stg_sample"
  where cram is not null
)

select 
 distinct 
 'sm' || '_' || md5('cmg_yale' || '|' || cast(coalesce(sample_id, '') as text))::text as "sample_id",
 'fl' || '_' || md5('cmg_yale' || '|' || cast(coalesce(name, '') as text))::text as "file_id"
from 
  get_sm_files 
  left join "dbt"."main_main"."cmg_yale_stg_file_inventory"
    on file = file_ref