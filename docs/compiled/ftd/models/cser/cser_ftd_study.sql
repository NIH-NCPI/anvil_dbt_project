

select 
  'phs002307'::text as "parent_study_id",
  title::text as "study_title",
    'st' || '_' || md5('cser' || '|' || cast(coalesce(dataset_id, '') as text))::text as "id"
from (select distinct title, dataset_id from "dbt"."main_main"."cser_stg_anvil_dataset") as ad