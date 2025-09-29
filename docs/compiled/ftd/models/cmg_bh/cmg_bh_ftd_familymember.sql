

select
  'fy' || '_' || md5('cmg_bh' || '|' || cast(coalesce(family_id, '') as text))::text as "family_id", 
  'sb' || '_' || md5('cmg_bh' || '|' || cast(coalesce(subject_id, '') as text))::text as "family_member",
  'ap' || '_' || md5('cmg_bh' || '|' || cast(coalesce(ingest_provenance, '') as text))::text as "has_access_policy",
  'fm' || '_' || md5('cmg_bh' || '|' || cast(coalesce(subject_id, '') as text))::text as "id",
    case 
    when family_relationship = 'Brother'
    then 'BRO'
    when family_relationship = 'Child'
    then 'CHILD'
    when family_relationship = 'Father'
    then 'FTH'
    when family_relationship = 'Grandchild'
    then 'GRNDCHILD'
    when family_relationship = 'Maternal Aunt'
    then 'MAUNT'
    when family_relationship = 'Maternal Cousin'
    then 'MCOUSN'
    when family_relationship = 'Maternal Grandmother'
    then 'MGRMTH'
    when family_relationship = 'Maternal Half Brother'
    then 'HBRO'
    when family_relationship = 'Maternal Uncle'
    then 'MUNCLE'
    when family_relationship = 'Monozygous Twin'
    then 'ITWIN'
    when family_relationship = 'Mother'
    then 'MTH'
    when family_relationship = 'Nephew'
    then 'NEPHEW'
    when family_relationship = 'Niece'
    then 'NIECE'
    when family_relationship = 'Niece''s Child'
    then 'EXT'
    when family_relationship = 'Paternal Cousin'
    then 'PCOUSN'
    when family_relationship = 'Paternal Uncle'
    then 'PUNCLE'
    when family_relationship = 'Sister'
    then 'SIS'
    when family_relationship is null then null
    else CONCAT('FTD_FLAG:unhandled family_role: ',family_relationship)
  end as "family_role"
from (select distinct family_id, subject_id, family_relationship, ingest_provenance from "dbt"."main_main"."cmg_bh_stg_subject") as s