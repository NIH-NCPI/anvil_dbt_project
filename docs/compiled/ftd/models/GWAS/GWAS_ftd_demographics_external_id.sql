

select 
  'dm' || '_' || md5('phs001584' || '|' || cast(coalesce(demographics.subject_id, '') as text))::text as "demographics_id",
  demographics.subject_id::text as "external_id"
from "dbt"."main_main"."GWAS_stg_demographics" as demographics