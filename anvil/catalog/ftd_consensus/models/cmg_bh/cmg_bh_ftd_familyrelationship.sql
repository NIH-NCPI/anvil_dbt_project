{{ config(materialized='table', schema='cmg_bh_data') }}

with
probands_only as (
    select
      ingest_provenance,
      family_id,
      subject_id,
      family_relationship as "proband_rel_code",
    from {{ ref('cmg_bh_stg_subject') }}
    where family_relationship = "Proband"
)
,others_only as (
    select
      ingest_provenance,
      family_id,
      subject_id,
      family_relationship as "other_rel_code",
    from {{ ref('cmg_bh_stg_subject') }}
    where family_relationship != "Proband"
)
,fr_base(
    select
      distinct
      {{ generate_global_id(prefix='fm',descriptor=['p.subject_id'], study_id='cmg_bh') }}::text as "family_member",
      proband_rel_code,
      {{ generate_global_id(prefix='fm',descriptor=['o.subject_id'], study_id='cmg_bh') }}::text as "other_family_member",
      other_rel_code,
      {{ generate_global_id(prefix='ap',descriptor=['p.ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
      {{ generate_global_id(prefix='fr',descriptor=['p.subject_id,o.subject_id'], study_id='cmg_bh') }}::text as "id"
    from probands_only p
    join others_only o
    on p.family_id = o.family_id
      and p.ingest_provenance = o.ingest_provenance
)
,fr_rev( --create the reverse family relationships and form relationship codes
    select
      family_member,
      other_family_member,
      has_access_policy,
      id,
        case 
    -- TODO proband to other relationships
        when proband_rel_code is null then null
        CONCAT('FTD_FLAG:unhandled proband_rel_code: ',proband_rel_code)
      end as "family_relationship_code"
    from fr_base
    
    union all
    
    select
      other_family_member as "family_member",
      family_member as "other_family_member",
      has_access_policy,
      id,
        case 
        when other_rel_code is not null then other_rel_code
        when other_rel_code is null then null
        else CONCAT('FTD_FLAG:unhandled other_rel_code: ',other_rel_code)
      end as "family_relationship_code"
    from fr_base
)
select distinct
  family_member,
  other_family_member,
  family_relationship_code
  has_access_policy,
  id
from fr_rev
