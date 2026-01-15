{{ config(materialized='table', schema='cmg_uwash_data') }}

with unioned_codes as (
    select 
    subject_id,
    affected_status as "value_display",
    UNNEST(IFNULL(SPLIT(REPLACE(disease_id, ';', ''), '|'),[NULL])) as code,
    disease_description as "display",
    'disease' as assertion_type
  from {{ ref('cmg_uwash_stg_subject') }}
 
 union 
 
 select 
    subject_id,
    affected_status as "value_display",
    UNNEST(IFNULL(SPLIT(REPLACE(hpo_present, ';', ''), '|'),[NULL])) as code,
    phenotype_description as "display",
    'phenotypic_feature' as assertion_type
  from {{ ref('cmg_uwash_stg_subject') }}
     ),
             
all_codes as (
    select
    uc.subject_id as subject_id,
    uc.value_display as value_display,
    CASE 
        WHEN cua.response_code NOT LIKE '%:%' THEN concat('OMIM:', cua.response_code)
        ELSE cua.response_code
    END as code,
    cua.display as display,
    uc.assertion_type,
    NULL as system
    from unioned_codes as uc
    join {{ ref('cmg_uwash_annotations') }} as cua
    on uc.code = cua.searched_code
         
    union all
    
    select
    subject_id,
    uc.value_display::text as "value_display",
    cm.code as code,
    cm.display as display,
    CASE "local code system"
        WHEN 'CMG/phenotype_description' THEN 'phenotypic_feature'
        WHEN 'CMG/disease_description' THEN 'disease'
    END::text as assertion_type,
    cm."code system" as system
    from unioned_codes as uc
    left join {{ ref('cmg_uwash_split_text_mappings_var') }} as st
    on trim(lower(uc.display)) = trim(lower(st.original_text))
    left join {{ ref('cmg_uwash_condition_mappings') }} as cm
    on trim(lower(st.split_text)) = trim(lower(cm."local code"))
              
    union all
        
    select 
    subject_id,
    s.value_display,
    cm.code,
    cm.display,
    CASE "local code system"
        WHEN 'CMG/phenotype_description' THEN 'phenotypic_feature'
        WHEN 'CMG/disease_description' THEN 'disease'
    END::text as assertion_type,
    cm."code system" as system
    from unioned_codes as s
    left join {{ ref('cmg_uwash_condition_mappings') }} as cm
    on trim(lower(s.display)) = trim(lower(cm."local code"))
    where s.code is null
),

all_conditions as (
    select 
    subject_id,
    s.value_display,
    cm.code,
    cm.display,
    CASE "local code system"
        WHEN 'CMG/phenotype_description' THEN 'phenotypic_feature'
        WHEN 'CMG/disease_description' THEN 'disease'
    END::text as assertion_type,
    cm."code system" as system
    from unioned_codes as s
    left join {{ ref('cmg_uwash_condition_mappings') }} as cm
    on trim(lower(s.display)) = trim(lower(cm."local code"))
    where s.code is null
    
    union all
    
    select 
    subject_id,
    value_display,
    code,
    display,
    assertion_type,
    system
    from all_codes as ac
   ),
   
all_formatted_codes as (
  select
  CASE
      WHEN code NOT LIKE '%:%' AND system ILIKE '%omim%' THEN concat('OMIM:', code)
      WHEN code NOT LIKE '%:%' AND system ILIKE '%snomed%' THEN concat('SNOMED:', code)
      ELSE code
  END as code,
  subject_id
from {{ ref('cmg_uwash_stg_subject') }} as s
left join all_conditions as alc
using(subject_id)
    )
        
select distinct
  {{ generate_global_id(prefix='sa',descriptor=['subject_id', 'code'], study_id='phs000693') }}::text as "subjectassertion_id",
  subject_id::text as "external_id"
from all_formatted_codes as afc
where code is not null