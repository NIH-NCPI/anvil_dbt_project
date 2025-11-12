{{ config(materialized='table', schema='cmg_uwash_data') }}

with unioned_codes as (
    select 
    subject_id,
    UNNEST(IFNULL(SPLIT(REPLACE(disease_id, ';', ''), '|'),[NULL])) as code,
    disease_description as "display",
  from {{ ref('cmg_uwash_stg_subject') }}
 
 union 
 
 select 
    subject_id,
    UNNEST(IFNULL(SPLIT(REPLACE(hpo_present, ';', ''), '|'),[NULL])) as code,
    phenotype_description as "display",
  from {{ ref('cmg_uwash_stg_subject') }}
     ),
          
all_codes as (select
    uc.subject_id as subject_id,
    CASE 
        WHEN cua.response_code NOT LIKE '%:%' THEN concat('OMIM:', cua.response_code)
        ELSE cua.response_code
    END as code,
    cua.display as display,
    NULL as system
    from unioned_codes as uc
    join {{ ref('cmg_uwash_annotations') }} as cua
    on uc.code = cua.searched_code
         
    union all
    
    select
    subject_id,
    cm.code as code,
    cm.display as display,
    cm."code system" as system
    from unioned_codes as uc
    left join {{ ref('cmg_uwash_condition_mappings') }} as cm
    on trim(lower(uc.display)) = trim(lower(cm."local code"))
              
    union all
        
    select 
    subject_id,
    cm.code,
    cm.display,
    cm."code system" as system
    from unioned_codes as s
    left join {{ ref('cmg_uwash_condition_mappings') }} as cm
    on trim(lower(s.display)) = trim(lower(cm."local code"))
    where s.code is null
),

all_conditions as (
    select 
    subject_id,
    cm.code,
    cm.display,
    cm."code system" as system
    from unioned_codes as s
    left join {{ ref('cmg_uwash_condition_mappings') }} as cm
    on trim(lower(s.display)) = trim(lower(cm."local code"))
    where s.code is null
    
    union all
    
    select 
    subject_id,
    code,
    display,
    system
    from all_codes as ac
   ),

concatenated_codes as (
    select
    subject_id,
     CASE
      WHEN code NOT LIKE '%:%' AND system ILIKE '%omim%' THEN concat('OMIM:', code)
      WHEN code NOT LIKE '%:%' AND system ILIKE '%snomed%' THEN concat('SNOMED:', code)
      ELSE code
  END as code,
 from  all_conditions as alc
 where code is not null
    )

select distinct
  {{ generate_global_id(prefix='sa',descriptor=['subject_id', 'code'], study_id='phs000693') }}::text as "subjectassertion_id",
    {{ generate_global_id(prefix='',descriptor=['project_id'], study_id='phs000693') }}::text as "source_data_id"
from {{ ref('cmg_uwash_stg_subject') }} as subject
left join concatenated_codes as cc
using(subject_id)

