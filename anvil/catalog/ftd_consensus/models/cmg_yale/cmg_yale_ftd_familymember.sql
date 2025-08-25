{{ config(materialized='table', schema='cmg_yale_data') }}

with 
lookup as (
    select 
      distinct 
      subject_id,
      ingest_provenance,
      family_id,
      proband_relationship,
      code as "tgt_role_code"
    from (select distinct subject_id, ingest_provenance, family_id, proband_relationship from {{ ref('cmg_yale_stg_subject') }}) s
         left join
         {{ ref('RoleCodeValueSet') }} as seed
         on seed.lower_exact_match_src_relationship_to_proband = lower(s.proband_relationship)
)

select 
  distinct
  {{ generate_global_id(prefix='sb',descriptor=['subject_id'], study_id='cmg_yale') }}::text as "family_member",
    case
    when tgt_role_code is not null 
    then tgt_role_code
    when tgt_role_code is null
    then concat('FTD_FLAG:unhandled family_role: ',proband_relationship)
  END::text as "family_role",
  {{ generate_global_id(prefix='ap',descriptor=['ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='fm',descriptor=['family_id','subject_id'], study_id='cmg_yale') }}::text as "id",
  {{ generate_global_id(prefix='fy',descriptor=['family_id'], study_id='cmg_bh') }}::text as "family_id"
from lookup