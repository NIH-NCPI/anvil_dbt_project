

select 
  NULL::text as "disease_limitation",
  NULL::text as "website", --TODO version of website
    case 
    when ingest_provenance like '%AnVIL_CMG_BaylorHopkins_HMB-IRB-NPU_WES%'
      then 'HMB-IRB-NPU'
    when ingest_provenance like '%AnVIL_CMG_BaylorHopkins_HMB-NPU_WES%'
      then 'HMB-NPU'
    else NULL
  end::text as "description",
  'ap' || '_' || md5('cmg_bh' || '|' || cast(coalesce(ingest_provenance, '') as text))::text as "id"
from (select distinct ingest_provenance from "dbt"."main_main"."cmg_bh_stg_subject") as s