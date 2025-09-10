

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
  p.subject_id as "external_id",
  'fr' || '_' || md5('cmg_bh' || '|' || cast(coalesce(family_id, '') as text) || '|' || 'cmg_bh' || '|' || cast(coalesce(p.subject_id, '') as text) || '|' || 'cmg_bh' || '|' || cast(coalesce(o.subject_id, '') as text) || '|' || 'cmg_bh' || '|' || cast(coalesce(other_rel_code, '') as text))::text as "familyrelationship_id"
from probands_only as p
left join others_only as o
using (family_id, ingest_provenance)
where o.subject_id is not null