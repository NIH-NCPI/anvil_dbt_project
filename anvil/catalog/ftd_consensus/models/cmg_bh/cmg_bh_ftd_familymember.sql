{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  {{ generate_global_id(prefix='sb',descriptor=['subject_id'], study_id='cmg_bh') }}::text as "family_member",
  case
    --            when 'Mother' then 'MTH'
    --             when 'Father' then 'FTH'
    --             when 'Brother' or 'Sister' then 'SIB'
    --             when 'Child' then 'CHILD'
    when family_relationship = 'Maternal Half Sibling' then 'HSIB'
    when family_relationship = 'Paternal Half Sibling' then 'HSIB'
    --             when 'Maternal Grandmother' then 'MGRMTH'
    when family_relationship = 'Maternal Grandfather' then 'MGRFTH'
    when family_relationship = 'Paternal Grandmother' then 'PGRMTH'
    when family_relationship = 'Paternal Grandfather' then 'PGRFTH'
    --             when 'Maternal Aunt' then 'MAUNT'
    when family_relationship = 'Paternal Aunt' then 'PAUNT'
    --             when 'Maternal Uncle' then 'MUNCLE'
    --             when 'Paternal Uncle' then 'PUNCLE'
    --             when 'Niece' then 'NIECE'
    --             when 'Nephew' then 'NEPHEW'
    when family_relationship = 'Maternal 1st Cousin' then 'MCOUSN'
    when family_relationship = 'Paternal 1st Cousin' then 'PCOUSN'
    --             when 'Proband' then 'SNOMED:85900004'
    when family_relationship = 'Grandchild' then '' --TODO
    when family_relationship = 'Maternal Cousin' then '' --TODO
    when family_relationship = 'Maternal Half Brother' then '' --TODO
    when family_relationship = 'Monozygous Twin' then '' --TODO
    when family_relationship = 'Niece\'s Child' then '' --TODO
    when family_relationship = 'Paternal Cousin' then '' --TODO
    else CONCAT('FTD_FLAG:unhandled family_role: ',family_relationship)
  end::text as "family_role", 
  {{ generate_global_id(prefix='',descriptor=['ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='fm',descriptor=['family_id','family_member'], study_id='cmg_bh') }}::text as "id",
  {{ generate_global_id(prefix='fy',descriptor=['family_id'], study_id='cmg_bh') }}::text as "family_id"
from (select subject_id, family_relationship, ingest_provenance{{ ref('cmg_bh_stg_subject') }}