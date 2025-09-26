

select 
  'sa' || '_' || md5('cmg_bh' || '|' || cast(coalesce(s.ingest_provenance, '') as text))::text as "accesspolicy_id",
  'controlled'::text as "data_access_type"
from (select distinct ingest_provenance from "dbt"."main_main"."cmg_bh_stg_subject") as s