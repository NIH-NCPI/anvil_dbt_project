

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
      p.subject_id,
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
  subject_id::text as "external_id",
  id as "familyrelationship_id",
from fr_base