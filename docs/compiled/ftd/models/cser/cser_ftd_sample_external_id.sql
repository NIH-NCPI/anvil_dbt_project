

select 
  'sm' || '_' || md5('cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(sample_id, '') as text))::text as "sample_id",
  sample_id::text as "external_id"
from  (select distinct subject_id, sample_id 
      from "dbt"."main_main"."cser_stg_sample"
      ) as sm