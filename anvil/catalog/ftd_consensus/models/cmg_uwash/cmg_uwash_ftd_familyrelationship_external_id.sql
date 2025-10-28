{{ config(materialized='table', schema='cmg_uwash_data') }}

with
probands_only as (
    select distinct
      consent_id,
      family_id,
      subject_id,
      proband_relationship as "proband_rel_code",
    from {{ ref('cmg_uwash_stg_subject') }}
    where proband_relationship = 'Proband'
)
,others_only as (
    select distinct
      consent_id,
      family_id,
      subject_id,
      proband_relationship as "other_rel_code",
    from {{ ref('cmg_uwash_stg_subject') }}
    where proband_relationship != 'Proband'
)
,fr_base as (
    select
      distinct
      p.subject_id as ftd_subject_1,
      o.subject_id as ftd_subject_2,
      {{ generate_global_id(prefix='sb',descriptor=['p.subject_id', 'consent_id'], study_id='phs000693') }}::text as "family_member",
      proband_rel_code,
      {{ generate_global_id(prefix='sb',descriptor=['o.subject_id', 'consent_id'], study_id='phs000693') }}::text as "other_family_member",
      other_rel_code,
      {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='phs000693') }}::text as "has_access_policy",
      {{ generate_global_id(prefix='fr',descriptor=['family_id','p.subject_id','o.subject_id','other_rel_code'], study_id='phs000693') }}::text as "id"
    from probands_only as p
    left join others_only as o
    using (family_id, consent_id)
    where o.subject_id is not null
), 

bio_curies as (
    select 
        lower(relationship_to_proband) as relationship_to_proband,
        max(case 
            when study_id = 'cmg_uwash' then curie 
        end)::text as bio_curie,
        max(case 
            when study_id is null or study_id != 'cmg_uwash' then curie 
        end)::text as def_curie
    from {{ ref('fr_relationship_code') }}
    group by lower(relationship_to_proband)
)

select 
  distinct
  id as "familyrelationship_id",
  ftd_subject_1::text as "external_id"
from fr_base
