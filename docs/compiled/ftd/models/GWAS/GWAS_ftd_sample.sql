

select DISTINCT
  NULL as "parent_sample",
  CASE WHEN sampleattributes.analyte_type = 'DNA' THEN 'LNC:LP18329-0'
       ELSE CONCAT('FTD_FLAG: unhandled sample_type: ',analyte_type)
  END::text as "sample_type",
  NULL as "availablity_status",
  NULL as "quantity_number",
  NULL as "quantity_units",
    'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "has_access_policy",
    'sm' || '_' || md5('phs001584' || '|' || cast(coalesce(samplesubjectmapping.subject_id, '') as text) || '|' || 'phs001584' || '|' || cast(coalesce(samplesubjectmapping.source_sampid, '') as text))::text as "id",
    'sb' || '_' || md5('phs001584' || '|' || cast(coalesce(samplesubjectmapping.subject_id, '') as text))::text as "subject_id",
    NULL as "biospecimen_collection_id"
from "dbt"."main_main"."GWAS_stg_sampleattributes" as sampleattributes
left join "dbt"."main_main"."GWAS_stg_samplesubjectmapping" as samplesubjectmapping
using (sample_id)
left join "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
using (subject_id)