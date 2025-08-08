{{ config(materialized='table', schema='cmg_bh_data') }}

select
  family_id,
  subject_id as "family_member",
  {{ generate_global_id(prefix='ap',descriptor=['ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='fm',descriptor=['subject_id'], study_id='cmg_bh') }}::text as "id",
    case 
    when other_rel_code = 'Brother'
    then 'BRO'
    when other_rel_code = 'Child'
    then 'CHILD'
    when other_rel_code = 'Father'
    then 'FTH'
    when other_rel_code = 'Grandchild'
    then 'GRNDCHILD'
    when other_rel_code = 'Maternal Aunt'
    then 'MAUNT'
    when other_rel_code = 'Maternal Cousin'
    then 'MCOUSN'
    when other_rel_code = 'Maternal Grandmother'
    then 'MGRMTH'
    when other_rel_code = 'Maternal Half Brother'
    then 'HBRO'
    when other_rel_code = 'Maternal Uncle'
    then 'MUNCLE'
    when other_rel_code = 'Monozygous Twin'
    then 'ITWIN'
    when other_rel_code = 'Mother'
    then 'MTH'
    when other_rel_code = 'Nephew'
    then 'NEPHEW'
    when other_rel_code = 'Niece'
    then 'NIECE'
    when other_rel_code = 'Niece''s Child'
    then 'EXT'
    when other_rel_code = 'Paternal Cousin'
    then 'PCOUSN'
    when other_rel_code = 'Paternal Uncle'
    then 'PUNCLE'
    when other_rel_code = 'Sister'
    then 'SIS'
    when other_rel_code is null then null
    else CONCAT('FTD_FLAG:unhandled family_relationship_code: ',other_rel_code)
  end as "family_role"
from (select distinct subject_id, family_relationship, ingest_provenance from {{ ref('cmg_bh_stg_subject') }}) as s