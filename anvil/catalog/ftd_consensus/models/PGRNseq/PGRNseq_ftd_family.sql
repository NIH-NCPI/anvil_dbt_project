{{ config(materialized='table', schema='PGRNseq_data') }}

select 
GEN_UNKNOWN.family_type::text as "family_type",
  GEN_UNKNOWN.family_description::text as "family_description",
  GEN_UNKNOWN.consanguinity::text as "consanguinity",
  GEN_UNKNOWN.family_study_focus::text as "family_study_focus",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='PGRNseq') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='PGRNseq') }}::text as "id"
from {{ ref('PGRNseq_stg_bmi') }} as bmi
join {{ ref('PGRNseq_stg_demographics') }} as demographics
on subjectconsent.subject_id = demographics.subject_id  join {{ ref('PGRNseq_stg_ecg') }} as ecg
on   join {{ ref('PGRNseq_stg_labs') }} as labs
on   join {{ ref('PGRNseq_stg_sampleattribution') }} as sampleattribution
on samplesubjectmapping.sample_id = sampleattribution.sample_id  join {{ ref('PGRNseq_stg_samplesubjectmapping') }} as samplesubjectmapping
on sampleattribution.sample_id = samplesubjectmapping.sample_id  join {{ ref('PGRNseq_stg_subjectconsent') }} as subjectconsent
on demographics.subject_id = subjectconsent.subject_id 

