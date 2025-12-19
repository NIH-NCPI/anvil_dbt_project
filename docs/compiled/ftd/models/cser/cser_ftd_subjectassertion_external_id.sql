

with hpo_codes as (
    select
    subject_id,
    consent_id,
    UNNEST(IFNULL(SPLIT(hpo_present, '|'),[NULL])) as code,
    from (select distinct subject_id, consent_id, hpo_present from "dbt"."main_main"."cser_stg_subject") as hp
    
    union all
    
    select
    subject_id,
    consent_id,
    UNNEST(IFNULL(SPLIT(hpo_absent, '|'),[NULL])) as code,
    from (select distinct subject_id, consent_id, hpo_absent from "dbt"."main_main"."cser_stg_subject") as ha
)

select 
  'sa' || '_' || md5('cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(response_code, '') as text))::text as "subjectassertion_id",
  subject_id::text as "external_id"
from hpo_codes as hc
join "dbt"."main"."cser_annotations" as ca
    on hc.code = ca.searched_code
where hc.code is not null