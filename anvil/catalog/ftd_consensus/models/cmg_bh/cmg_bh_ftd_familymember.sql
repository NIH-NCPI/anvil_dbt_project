{{ config(materialized='table', schema='cmg_bh_data') }}

with 
source as (
    select 
      {{ generate_global_id(prefix='sb',descriptor=['s.subject_id'], study_id='cmg_bh') }}::text as "family_member",
      case s.family_relationship
        --            when 'Mother' then 'MTH'
        --             when 'Father' then 'FTH'
        --             when 'Brother' or 'Sister' then 'SIB'
        --             when 'Child' then 'CHILD'
        when 'Maternal Half Sibling' then 'HSIB'
        when 'Paternal Half Sibling' then 'HSIB'
        --             when 'Maternal Grandmother' then 'MGRMTH'
        when 'Maternal Grandfather' then 'MGRFTH'
        when 'Paternal Grandmother' then 'PGRMTH'
        when 'Paternal Grandfather' then 'PGRFTH'
        --             when 'Maternal Aunt' then 'MAUNT'
        when 'Paternal Aunt' then 'PAUNT'
        --             when 'Maternal Uncle' then 'MUNCLE'
        --             when 'Paternal Uncle' then 'PUNCLE'
        --             when 'Niece' then 'NIECE'
        --             when 'Nephew' then 'NEPHEW'
        when 'Maternal 1st Cousin' then 'MCOUSN'
        when 'Paternal 1st Cousin' then 'PCOUSN'
        --             when 'Proband' then 'SNOMED:85900004'
        when 'Grandchild' then '' --TODO
        when 'Maternal Cousin' then '' --TODO
        when 'Maternal Half Brother' then '' --TODO
        when 'Monozygous Twin' then '' --TODO
        when 'Niece\'s Child' then '' --TODO
        when 'Paternal Cousin' then '' --TODO
        else then NULL
        end::text as "family_role", 
      {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "has_access_policy",
      {{ generate_global_id(prefix='fm',descriptor=['s.family_id','s.family_member'], study_id='cmg_bh') }}::text as "id",
      {{ generate_global_id(prefix='fy',descriptor=['s.family_id'], study_id='cmg_bh') }}::text as "family_id"
    from {{ ref('cmg_bh_stg_subject') }} as s
)

select 
  source.family_member, --TODO join col or actual id
  source.family_role, --TODO get enums ooor are these supposed to be the MD codes?
  source.has_access_policy,
  source.id,
  source.family_id
from source
    