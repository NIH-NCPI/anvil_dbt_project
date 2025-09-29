

select 
  'st' || '_' || md5('cmg_yale' || '|' || cast(coalesce(registered_identifier, '') as text))::text as "study_id",
  NULL::text as "funding_source"
from (select distinct registered_identifier 
      from "dbt"."main_main"."cmg_yale_stg_anvil_dataset" 
      where registered_identifier is not null
     ) as s