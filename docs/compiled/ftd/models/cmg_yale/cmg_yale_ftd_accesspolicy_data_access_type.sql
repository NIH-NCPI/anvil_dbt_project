

select 
  'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_group, '') as text))::text as "accesspolicy_id",
  data_access_type,
  consent_group as "ftd_consent_group"
from (select distinct consent_group, data_access_type
      from "dbt"."main_main"."cmg_yale_stg_anvil_dataset" as ad
      left join "dbt"."main"."ap_access_policy" as cc
      on cc.consent_code = ad.consent_group
      ) as s