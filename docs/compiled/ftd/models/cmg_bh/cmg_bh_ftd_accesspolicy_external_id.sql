

select 
  'ap' || '_' || md5('cmg_bh' || '|' || cast(coalesce(ingest_provenance, '') as text))::text as "accesspolicy_id",
  ingest_provenance::text as "external_id"
from (select distinct ingest_provenance from "dbt"."main_main"."cmg_bh_stg_subject") as s