

select 
  distinct
  disease_limitation,
  consent_group as "ftd_consent_group",
  consent_description::text as "description",
  NULL::text as "website",
  'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_group, '') as text))::text as "id"
from (select distinct ad.consent_group, cc.consent_description, cc.disease_limitation
      from "dbt"."main_main"."cmg_yale_stg_anvil_dataset" as ad
      left join "dbt"."main"."ap_access_policy" as cc
      on cc.consent_code = ad.consent_group
      ) as s