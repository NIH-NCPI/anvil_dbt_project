
  
    
    

    create  table
      "dbt"."main_cmg_bh_data"."cmg_bh_ftd_familyrelationship__dbt_tmp"
  
    as (
      
-- Does not take into account multiple probands per family
-- left join - are there any others that don't have a proband
with
probands_only as (
    select distinct
      ingest_provenance,
      family_id,
      subject_id,
      family_relationship as "proband_rel_code",
    from "dbt"."main_main"."cmg_bh_stg_subject"
    where family_relationship = 'Proband'
)
,others_only as (
    select distinct
      ingest_provenance,
      family_id,
      subject_id,
      family_relationship as "other_rel_code",
    from "dbt"."main_main"."cmg_bh_stg_subject"
    where family_relationship != 'Proband'
)
,fr_base as (
    select
      distinct
      'sb' || '_' || md5('cmg_bh' || '|' || cast(coalesce(p.subject_id, '') as text))::text as "family_member",
      proband_rel_code,
      'sb' || '_' || md5('cmg_bh' || '|' || cast(coalesce(o.subject_id, '') as text))::text as "other_family_member",
      other_rel_code,
      'ap' || '_' || md5('cmg_bh' || '|' || cast(coalesce(p.ingest_provenance, '') as text))::text as "has_access_policy",
      'fr' || '_' || md5('cmg_bh' || '|' || cast(coalesce(p.subject_id, '') as text) || '|' || 'cmg_bh' || '|' || cast(coalesce(o.subject_id, '') as text))::text as "id"
    from probands_only as p
    left join others_only as o
    on p.family_id = o.family_id
      and p.ingest_provenance = o.ingest_provenance
    where o.subject_id is not null
)
select
  distinct
  family_member,
  other_family_member,
  has_access_policy,
  id,
    case 
    when other_rel_code = 'Brother'
    then 'KIN:024'
    when other_rel_code = 'Child'
    then 'KIN:032'
    when other_rel_code = 'Father'
    then 'KIN:028'
    when other_rel_code = 'Grandchild'
    then 'KIN:036'
    when other_rel_code = 'Maternal Aunt'
    then 'KIN:060'
    when other_rel_code = 'Maternal Cousin'
    then 'KIN:015'
    when other_rel_code = 'Maternal Grandmother'
    then 'KIN:052'
    when other_rel_code = 'Maternal Half Brother'
    then 'KIN:012'
    when other_rel_code = 'Maternal Uncle'
    then 'KIN:058'
    when other_rel_code = 'Monozygous Twin'
    then 'KIN:010'
    when other_rel_code = 'Mother'
    then 'KIN:027'
    when other_rel_code = 'Nephew'
    then 'KIN:046'
    when other_rel_code = 'Niece'
    then 'KIN:046'
    when other_rel_code = 'Niece''s Child' --TODO esc works?
    then 'KIN:001' -- isRelativeOf
    when other_rel_code = 'Paternal Cousin'
    then 'KIN:016'
    when other_rel_code = 'Paternal Uncle'
    then 'KIN:059'
    when other_rel_code = 'Sister'
    then 'KIN:024'
    when other_rel_code is null then null
    else CONCAT('FTD_FLAG:unhandled relationship_code: ',other_rel_code)
  end as "relationship_code"
from fr_base
    );
  
  