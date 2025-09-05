{{ config(materialized='table', schema='PGRNseq_data') }}

select 
  {{ generate_global_id(prefix='',descriptor=[''], study_id='PGRNseq') }}::text as "sourcedata_id",
  GEN_UNKNOWN.query_parameter::text as "query_parameter"
from {{ ref('PGRNseq_stg_bmi') }} as bmi
join {{ ref('PGRNseq_stg_demographics') }} as demographics
on subjectconsent.subject_id = demographics.subject_id  join {{ ref('PGRNseq_stg_ecg') }} as ecg
on   join {{ ref('PGRNseq_stg_labs') }} as labs
on   join {{ ref('PGRNseq_stg_sampleattribution') }} as sampleattribution
on samplesubjectmapping.sample_id = sampleattribution.sample_id  join {{ ref('PGRNseq_stg_samplesubjectmapping') }} as samplesubjectmapping
on sampleattribution.sample_id = samplesubjectmapping.sample_id  join {{ ref('PGRNseq_stg_subjectconsent') }} as subjectconsent
on demographics.subject_id = subjectconsent.subject_id 

