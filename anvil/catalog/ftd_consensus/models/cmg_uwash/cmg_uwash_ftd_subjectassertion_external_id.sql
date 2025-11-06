{{ config(materialized='table', schema='cmg_uwash_data') }}

with unioned_codes as (
    select 
    subject_id,
    UNNEST(SPLIT(replace(disease_id, ';',''), '|')) as code,
    disease_description as "display",
  from {{ ref('cmg_uwash_stg_subject') }}
 
 union all
 
 select 
    subject_id,
    UNNEST(SPLIT(replace(hpo_present, ';',''), '|')) as code,
    phenotype_description as "display",
  from {{ ref('cmg_uwash_stg_subject') }}
     ),
     
mapped_codes as (
    select
    subject_id,
    cm.code as code,
    cm.display as display,
    from unioned_codes as uc
    left join {{ ref('cmg_uwash_condition_mappings') }} as cm
    on lower(uc.display) = lower(cm."local code")
    
    union all
    
    select 
    subject_id,
    code,
    display,
    from unioned_codes as uc
),
    
annotated_codes as (
    select 
    mc.subject_id as subject_id,
    mc.code as code,
    cua.display as display,
    from mapped_codes as mc
    join {{ ref('cmg_uwash_annotations') }} as cua
    on mc.code = cua.searched_code
    ),
    
all_unioned_codes as (
    select distinct
        mc.subject_id,
        mc.code,
        coalesce(ac.display, mc.display) as display,
    from mapped_codes mc
    left join annotated_codes as ac
        on mc.subject_id = ac.subject_id
        and mc.code = ac.code
)
select distinct
  {{ generate_global_id(prefix='sa',descriptor=['subject_id', 'code'], study_id='phs000693') }}::text as "subjectassertion_id",
  subject_id::text as "external_id"
from {{ ref('cmg_uwash_stg_subject') }} as s
left join all_unioned_codes as auc
using(subject_id)
where code is not null