
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
  'fy' || '_' || md5('cmg_bh' || '|' || cast(coalesce(o.family_id, '') as text))::text as "family_id",
  'fr' || '_' || md5('cmg_bh' || '|' || cast(coalesce(p.subject_id, '') as text) || '|' || 'cmg_bh' || '|' || cast(coalesce(o.subject_id, '') as text))::text as "family_relationships_id"
from probands_only p
left join others_only o
on p.family_id = o.family_id
  and p.ingest_provenance = o.ingest_provenance
where o.subject_id is not null