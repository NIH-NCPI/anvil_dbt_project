

select 
  'dm' || '_' || md5('cmg_bh' || '|' || cast(coalesce(subject_id, '') as text))::text as "demographics_id",
  'sd' || '_' || md5('cmg_bh' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "source_data_id"
from (select distinct subject_id, dbgap_study_id from "dbt"."main_main"."cmg_bh_stg_subject" ) as s