 

select 
  NULL::text as "family_type",
  NULL::text as "family_description",
  
  code::text as "consanguinity",
  coalesce(consanguinity, 'FTD_NULL') AS "ftd_consanguinity", -- flag nulls for analysis
  coalesce(code, 'Needs Handling') AS "ftd_flag_consanguinity", -- flag unhandled strings
  
  NULL::text as "family_study_focus",
  'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
  'fy' || '_' || md5('cmg_yale' || '|' || cast(coalesce(family_id, '') as text))::text as "id"
from (select 
        distinct consent_id, family_id, code, consanguinity
      from "dbt"."main_main"."cmg_yale_stg_family"
      left join "dbt"."main"."fm_consanguinity"
      on lower(consanguinity) = src_format
      ) as s