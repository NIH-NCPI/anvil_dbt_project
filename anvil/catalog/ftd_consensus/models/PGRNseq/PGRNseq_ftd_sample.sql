{{ config(materialized='table', schema='PGRNseq_data') }}

select 
  {{ generate_global_id(prefix='',descriptor=[''], study_id='PGRNseq') }}::text as "parent_sample",
  GEN_UNKNOWN.sample_type::text as "sample_type",
  GEN_UNKNOWN.availablity_status::text as "availablity_status",
  GEN_UNKNOWN.quantity_number::text as "quantity_number",
  GEN_UNKNOWN.quantity_units::text as "quantity_units",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='PGRNseq') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='PGRNseq') }}::text as "id",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='PGRNseq') }}::text as "subject_id",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='PGRNseq') }}::text as "biospecimen_collection_id"
from {{ ref('PGRNseq_stg_bmi') }} as bmi
join {{ ref('PGRNseq_stg_demographics') }} as demographics
on subjectconsent.subject_id = demographics.subject_id  join {{ ref('PGRNseq_stg_ecg') }} as ecg
on   join {{ ref('PGRNseq_stg_labs') }} as labs
on   join {{ ref('PGRNseq_stg_sampleattribution') }} as sampleattribution
on samplesubjectmapping.sample_id = sampleattribution.sample_id  join {{ ref('PGRNseq_stg_samplesubjectmapping') }} as samplesubjectmapping
on sampleattribution.sample_id = samplesubjectmapping.sample_id  join {{ ref('PGRNseq_stg_subjectconsent') }} as subjectconsent
on demographics.subject_id = subjectconsent.subject_id 

