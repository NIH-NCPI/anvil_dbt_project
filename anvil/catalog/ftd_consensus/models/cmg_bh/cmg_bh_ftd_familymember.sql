{{ config(materialized='table', schema='cmg_bh_data') }}

with
probands_only as (
    select distinct
      ingest_provenance,
      family_id,
      subject_id,
      family_relationship as "proband_rel_code",
    from {{ ref('cmg_bh_stg_subject') }}
    where family_relationship = 'Proband'
)
,others_only as (
    select distinct
      ingest_provenance,
      family_id,
      subject_id,
      family_relationship as "other_rel_code",
    from {{ ref('cmg_bh_stg_subject') }}
    where family_relationship != 'Proband'
)
,fr_base as (
    select
      distinct
      p.family_id,
      {{ generate_global_id(prefix='sb',descriptor=['p.subject_id'], study_id='cmg_bh') }}::text as "family_member",
      proband_rel_code,
      {{ generate_global_id(prefix='sb',descriptor=['o.subject_id'], study_id='cmg_bh') }}::text as "other_family_member",
      other_rel_code,
      {{ generate_global_id(prefix='ap',descriptor=['p.ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
      {{ generate_global_id(prefix='fm',descriptor=['p.subject_id','o.subject_id'], study_id='cmg_bh') }}::text as "id"
    from probands_only p
    join others_only o
    on p.family_id = o.family_id
      and p.ingest_provenance = o.ingest_provenance
)
select
  family_id,
  family_member,
  other_family_member,
  has_access_policy,
  id,
    case 
    when proband_rel_code = 'Brother'
    then 'SIB'
    when proband_rel_code = 'Child'
    then 'PRN'
    when proband_rel_code = 'Father'
    then 'CHILD'
    when proband_rel_code = 'Grandchild'
    then 'GRPRN'
    when proband_rel_code = 'Maternal Aunt'
    then 'NIENEPH'
    when proband_rel_code = 'Maternal Cousin'
    then 'COUSN'
    when proband_rel_code = 'Maternal Grandmother'
    then 'GRNDCHILD'
    when proband_rel_code = 'Maternal Half Brother'
    then 'HBRO'
    when proband_rel_code = 'Maternal Uncle'
    then 'NIENEPH'
    when proband_rel_code = 'Monozygous Twin'
    then 'ITWIN'
    when proband_rel_code = 'Mother'
    then 'CHILD'
    when proband_rel_code = 'Nephew'
    then 'EXT'
    when proband_rel_code = 'Niece'
    then 'EXT'
    when proband_rel_code = 'Niece''s Child'
    then 'EXT'
    when proband_rel_code = 'Paternal Cousin'
    then 'COUSN'
    when proband_rel_code = 'Paternal Uncle'
    then 'NIENEPH'
    when proband_rel_code = 'Sister'
    then 'SIB'
    when proband_rel_code is null then null
    else CONCAT('FTD_FLAG:unhandled family_relationship_code: ',proband_rel_code)
  end as "family_relationship_code"
from fr_base