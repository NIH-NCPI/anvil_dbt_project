

select 
  distinct
  'sm' || '_' || md5('cmg_yale' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cmg_yale' || '|' || cast(coalesce(sample_id, '') as text))::text as "sample_id",
  NULL::text as "storage_method"
from (select distinct subject_id,sample_id
      from "dbt"."main_main"."cmg_yale_stg_sample"
      ) as sam