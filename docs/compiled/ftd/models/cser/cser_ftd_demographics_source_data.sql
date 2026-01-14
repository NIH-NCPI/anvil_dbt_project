

select distinct
  'dm' || '_' || md5('cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "demographics_id",
    'sd' || '_' || md5('cser' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "source_data_id"
from "dbt"."main_main"."cser_stg_subject" as subject