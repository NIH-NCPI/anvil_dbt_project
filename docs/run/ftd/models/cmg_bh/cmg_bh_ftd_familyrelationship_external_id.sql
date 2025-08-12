
  
    
    

    create  table
      "dbt"."main_cmg_bh_data"."cmg_bh_ftd_familyrelationship_external_id__dbt_tmp"
  
    as (
      

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

select
  distinct
  CONCAT(p.subject_id, '|', o.subject_id) as "external_id",
  'fr' || '_' || md5('cmg_bh' || '|' || cast(coalesce(p.subject_id, '') as text) || '|' || 'cmg_bh' || '|' || cast(coalesce(o.subject_id, '') as text))::text as "familyrelationship_id"
from probands_only as p
left join others_only as o
on p.family_id = o.family_id
  and p.ingest_provenance = o.ingest_provenance
where o.subject_id is not null
    );
  
  