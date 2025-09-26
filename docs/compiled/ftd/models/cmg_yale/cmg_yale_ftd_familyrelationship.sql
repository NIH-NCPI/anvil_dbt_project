

with
probands_only as (
    select distinct
      consent_id,
      family_id,
      subject_id,
      proband_relationship as "proband_rel_code",
    from "dbt"."main_main"."cmg_yale_stg_subject"
    where proband_relationship = 'Proband'
)
,others_only as (
    select distinct
      consent_id,
      family_id,
      subject_id,
      proband_relationship as "other_rel_code",
    from "dbt"."main_main"."cmg_yale_stg_subject"
    where proband_relationship != 'Proband'
)
,fr_base as (
    select
      distinct
      p.subject_id as ftd_subject_1,
      o.subject_id as ftd_subject_2,
      'sb' || '_' || md5('cmg_yale' || '|' || cast(coalesce(p.subject_id, '') as text))::text as "family_member",
      proband_rel_code,
      'sb' || '_' || md5('cmg_yale' || '|' || cast(coalesce(o.subject_id, '') as text))::text as "other_family_member",
      other_rel_code,
      'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
      'fr' || '_' || md5('cmg_yale' || '|' || cast(coalesce(family_id, '') as text) || '|' || 'cmg_yale' || '|' || cast(coalesce(p.subject_id, '') as text) || '|' || 'cmg_yale' || '|' || cast(coalesce(o.subject_id, '') as text) || '|' || 'cmg_yale' || '|' || cast(coalesce(other_rel_code, '') as text))::text as "id"
    from probands_only as p
    left join others_only as o
    using (family_id, consent_id)
    where o.subject_id is not null
)

select 
  distinct
  family_member,
  other_family_member,
  has_access_policy,
  id,
  ftd_subject_1,
  ftd_subject_2,
  curie::text as "relationship_code",
  coalesce(other_rel_code, 'FTD_NULL') AS "ftd_family_rel", -- flag nulls for analysis
  coalesce(curie, 'Needs Handling - nullable') AS "ftd_flag_family_rel" -- flag unhandled strings
  
from fr_base
     left join
      (select 
       curie, 
       relationship_to_proband
       from "dbt"."main"."fr_relationship_code"
       ) as seed
     on relationship_to_proband = lower(fr_base.other_rel_code)