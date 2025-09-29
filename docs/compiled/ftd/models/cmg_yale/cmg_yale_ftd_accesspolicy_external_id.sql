

select 
  'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_group, '') as text))::text as "accesspolicy_id",
  consent_group::text as "external_id"
from (select distinct consent_group from "dbt"."main_main"."cmg_yale_stg_anvil_dataset") as s