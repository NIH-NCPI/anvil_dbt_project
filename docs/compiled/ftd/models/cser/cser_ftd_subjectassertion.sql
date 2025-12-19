


with hpo_codes as (
    select 
    subject_id,
    consent_id,
    UNNEST(IFNULL(SPLIT(hpo_present, '|'),[NULL])) as code,
    'LA9633-4'::text as "value_code",
    'Present'::text as "value_display"
    from (select distinct subject_id, consent_id, hpo_present from "dbt"."main_main"."cser_stg_subject")
    
    union all
    
    select 
    subject_id,
    consent_id,
    UNNEST(IFNULL(SPLIT(hpo_absent, '|'),[NULL])) as code,
    'LA9634-2'::text as "value_code",
    'Absent'::text as "value_display"
    from (select distinct subject_id, consent_id, hpo_absent from "dbt"."main_main"."cser_stg_subject")
)

select
'phenotypic_feature'::text as "assertion_type",
NULL::text as "age_at_assertion",
NULL::text as "age_at_event",
NULL::text as "age_at_resolution",
ca.response_code as "code",
ca.display::text as "display",
hc.value_code,
hc.value_display,
NULL::text as "value_number",
NULL::text as "value_units",
NULL::text as "value_units_display",
    'ap' || '_' || md5('cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
    'sa' || '_' || md5('cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(response_code, '') as text))::text as "id",
    'sb' || '_' || md5('cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "subject_id"
from hpo_codes as hc
join "dbt"."main"."cser_annotations" as ca
    on hc.code = ca.searched_code
where hc.code is not null