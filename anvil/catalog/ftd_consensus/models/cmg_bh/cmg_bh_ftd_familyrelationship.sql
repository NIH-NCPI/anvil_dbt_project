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
      {{ generate_global_id(prefix='sb',descriptor=['p.subject_id'], study_id='cmg_bh') }}::text as "family_member",
      proband_rel_code,
      {{ generate_global_id(prefix='sb',descriptor=['o.subject_id'], study_id='cmg_bh') }}::text as "other_family_member",
      other_rel_code,
      {{ generate_global_id(prefix='ap',descriptor=['p.ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
      {{ generate_global_id(prefix='fr',descriptor=['p.subject_id','o.subject_id'], study_id='cmg_bh') }}::text as "id"
    from probands_only p
    join others_only o
    on p.family_id = o.family_id
      and p.ingest_provenance = o.ingest_provenance
)
select
  family_member,
  other_family_member,
  has_access_policy,
  id,
    case 
    when proband_rel_code = 'Brother'
    then 'KIN:024'
    when proband_rel_code = 'Child'
    then 'KIN:032'
    when proband_rel_code = 'Father'
    then 'KIN:028'
    when proband_rel_code = 'Grandchild'
    then 'KIN:036'
    when proband_rel_code = 'Maternal Aunt'
    then 'KIN:060'
    when proband_rel_code = 'Maternal Cousin'
    then 'KIN:015'
    when proband_rel_code = 'Maternal Grandmother'
    then 'KIN:052'
    when proband_rel_code = 'Maternal Half Brother'
    then 'KIN:012'
    when proband_rel_code = 'Maternal Uncle'
    then 'KIN:058'
    when proband_rel_code = 'Monozygous Twin'
    then 'KIN:010'
    when proband_rel_code = 'Mother'
    then 'KIN:027'
    when proband_rel_code = 'Nephew'
    then 'KIN:001' --isRelativeOf
    when proband_rel_code = 'Niece'
    then 'KIN:001' --isRelativeOf
    when proband_rel_code = 'Niece''s Child'
    then 'KIN:001' --isRelativeOf
    when proband_rel_code = 'Paternal Cousin'
    then 'KIN:016'
    when proband_rel_code = 'Paternal Uncle'
    then 'KIN:059'
    when proband_rel_code = 'Sister'
    then 'KIN:024'
    when proband_rel_code is null then null
    else CONCAT('FTD_FLAG:unhandled family_relationship_code: ',proband_rel_code)
  end as "family_relationship_code"
from fr_base
