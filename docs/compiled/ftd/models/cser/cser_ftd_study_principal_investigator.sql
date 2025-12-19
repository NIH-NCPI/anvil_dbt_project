

select 
  'st' || '_' || md5('cser' || '|' || cast(coalesce(dataset_id, '') as text))::text as "study_id",
  principal_investigator::text as "principal_investigator"
from (select distinct dataset_id, principal_investigator from "dbt"."main_main"."cser_stg_anvil_dataset") as ad