

select 
  distinct
  coalesce(curie, 'FAMMEMB')::text as "family_role",
  coalesce(proband_relationship, 'FTD_NULL') AS "ftd_family_role", -- flag nulls for analysis
  coalesce(curie, 'Needs Handling') AS "ftd_flag_family_role", -- flag unhandled strings
  'sb' || '_' || md5('cmg_yale' || '|' || cast(coalesce(subject_id, '') as text))::text as "family_member",
  'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
  'fm' || '_' || md5('cmg_yale' || '|' || cast(coalesce(family_id, '') as text) || '|' || 'cmg_yale' || '|' || cast(coalesce(subject_id, '') as text))::text as "id",
  'fy' || '_' || md5('cmg_yale' || '|' || cast(coalesce(family_id, '') as text))::text as "family_id"
from (select distinct subject_id, consent_id, family_id, curie, relationship_to_proband, proband_relationship
      from "dbt"."main_main"."cmg_yale_stg_subject"
      left join "dbt"."main"."fm_family_role"
      on relationship_to_proband = lower(proband_relationship)
      ) as s