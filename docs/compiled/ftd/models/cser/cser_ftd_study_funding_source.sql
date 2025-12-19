

select 
  'st' || '_' || md5('cser' || '|' || cast(coalesce(dataset_id, '') as text))::text as "study_id",
  NULL::text as "funding_source"
from (select distinct dataset_id from "dbt"."main_main"."cser_stg_anvil_dataset") as ad