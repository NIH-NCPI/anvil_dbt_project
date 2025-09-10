

select
  distinct
  'fm' || '_' || md5('cmg_bh' || '|' || cast(coalesce(family_id, '') as text))::text as "family_id",
  family_id::text as "external_id"
from (select distinct family_id, ingest_provenance from "dbt"."main_main"."cmg_bh_stg_subject")