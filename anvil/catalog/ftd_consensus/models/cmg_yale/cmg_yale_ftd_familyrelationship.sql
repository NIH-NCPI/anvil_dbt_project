{{ config(materialized='table', schema='cmg_yale_data') }}

with
probands_only as (
    select distinct
      ingest_provenance,
      family_id,
      subject_id,
      proband_relationship as "proband_rel_code",
    from {{ ref('cmg_yale_stg_subject') }}
    where proband_relationship = 'Proband'
)
,others_only as (
    select distinct
      ingest_provenance,
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
      {{ generate_global_id(prefix='ap',descriptor=['ingest_provenance'], study_id='cmg_yale') }}::text as "has_access_policy",
      {{ generate_global_id(prefix='fr',descriptor=['family_id','p.subject_id','o.subject_id','other_rel_code'], study_id='cmg_yale') }}::text as "id"
    from probands_only as p
    left join others_only as o
    using (family_id, ingest_provenance)
    where o.subject_id is not null
)

select 
  distinct
  family_member,
  other_family_member,
  has_access_policy,
  id,
    case
    when tgt_role_code is not null then tgt_role_code
    when tgt_role_code is null concat('FTD_FLAG:unhandled relationship_code: ',proband_relationship)
  END::text as "relationship_code",
from fr_base
left join
  (select code from {{ ref('kin-to-fhir-FamilyMember') }}) as seed
on seed.lower_exact_match_src_relationship_to_proband = lower(s.proband_relationship)         