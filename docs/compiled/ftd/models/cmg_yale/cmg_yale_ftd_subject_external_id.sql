

select 
    'sb' || '_' || md5('cmg_yale' || '|' || cast(coalesce(subject_id, '') as text))::text as "subject_id",
    subject_id::text as "external_id"
from (select distinct subject_id, consent_id from "dbt"."main_main"."cmg_yale_stg_subject" ) AS s