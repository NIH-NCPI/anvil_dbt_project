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

select
  distinct
  CONCAT(p.subject_id, '|', o.subject_id) as "external_id",
  {{ generate_global_id(prefix='fr',descriptor=['p.subject_id','o.subject_id'], study_id='cmg_bh') }}::text as "familyrelationship_id"
from probands_only as p
left join others_only as o
on p.family_id = o.family_id
  and p.ingest_provenance = o.ingest_provenance
where o.subject_id is not null