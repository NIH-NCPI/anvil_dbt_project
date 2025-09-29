

select 
  'st' || '_' || md5('cmg_yale' || '|' || cast(coalesce(registered_identifier, '') as text))::text::text as "study_id",
  principal_investigator::text as "principal_investigator"
from (
    select distinct principal_investigator, registered_identifier 
    from "dbt"."main_main"."cmg_yale_stg_anvil_dataset"
    ) as s