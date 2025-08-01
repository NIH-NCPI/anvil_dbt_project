{{ config(materialized='table', schema='cmg_bh_data') }}

with
sub_slice as (
    select 
      distinct ingest_provenance
    from {{ ref('cmg_bh_stg_subject') }}  
)
,source as (
    select 
        case 
        when s.ingest_provenance like '%AnVIL_CMG_BaylorHopkins_HMB-IRB-NPU_WES%'
          then 'CMG BaylorHopkins HMB-IRB-NPU WES'
        when s.ingest_provenance like '%AnVIL_CMG_BaylorHopkins_HMB-NPU_WES%'
          then 'CMG BaylorHopkins HMB-NPU WES'
        else NULL
      end::text as "description",
      {{ generate_global_id(prefix='ap',descriptor=['s.ingest_provenance'], study_id='cmg_bh') }}::text as "id"
    from sub_slice as s
)

select 
  source.description as 'description',
  NULL::text as "disease_limitation",
  NULL::text as "website",
  source.id as 'id'
from source
    