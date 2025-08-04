{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  sample_source::text as "code",
  lookup.display::text as "display",
  NULL::text as "value_code",
  NULL::text as "value_display",
  NULL::text as "value_number",
  NULL::text as "value_units",
  NULL::text as "value_units_display",
  {{ generate_global_id(prefix='ap',descriptor=['ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='sd',descriptor=['sample_source'], study_id='cmg_bh') }}::text as "id"
from (select distinct ingest_provenance, sample_source {{ ref('cmg_bh_stg_sample') }} as sample
left join {{ ref('src_metadata') }} as lookup   