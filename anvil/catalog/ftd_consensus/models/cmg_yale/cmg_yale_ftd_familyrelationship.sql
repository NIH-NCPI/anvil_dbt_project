{{ config(materialized='table', schema='cmg_yale_data') }}

with
probands_only as (
    select distinct
      consent_id,
      family_id,
      subject_id,
      proband_relationship as "proband_rel_code",
    from {{ ref('cmg_yale_stg_subject') }}
    where proband_relationship = 'Proband'
)
,others_only as (
    select distinct
      consent_id,
      family_id,
      subject_id,
      proband_relationship as "other_rel_code",
    from {{ ref('cmg_yale_stg_subject') }}
    where proband_relationship != 'Proband'
)
,fr_base as (
    select
      distinct
      {{ generate_global_id(prefix='sb',descriptor=['p.subject_id'], study_id='cmg_yale') }}::text as "family_member",
      proband_rel_code,
      {{ generate_global_id(prefix='sb',descriptor=['o.subject_id'], study_id='cmg_yale') }}::text as "other_family_member",
      other_rel_code,
      {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='cmg_yale') }}::text as "has_access_policy",
      {{ generate_global_id(prefix='fr',descriptor=['family_id','p.subject_id','o.subject_id','other_rel_code'], study_id='cmg_yale') }}::text as "id"
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
    case
    when tgt_rel_code is not null
    then tgt_rel_code
    when tgt_rel_code is null 
    then concat('FTD_FLAG:unhandled relationship_code: ',other_rel_code)
  END::text as "relationship_code",
from fr_base
     left join
     (select 
       code as tgt_rel_code, 
       lower_exact_match_src_relationship_to_proband 
      from {{ ref('kin-to-fhir-FamilyMember') }}) as seed
      on seed.lower_exact_match_src_relationship_to_proband = lower(fr_base.other_rel_code)         