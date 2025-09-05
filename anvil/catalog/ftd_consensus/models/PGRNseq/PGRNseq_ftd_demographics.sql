{{ config(materialized='table', schema='PGRNseq_data') }}

select 
GEN_UNKNOWN.date_of_birth::integer as "date_of_birth",
  GEN_UNKNOWN.date_of_birth_type::text as "date_of_birth_type",
  demographics.sex::text as "sex",
  GEN_UNKNOWN.sex_display::text as "sex_display",
  GEN_UNKNOWN.race_display::text as "race_display",
  demographics.ethnicity::text as "ethnicity",
  GEN_UNKNOWN.ethnicity_display::text as "ethnicity_display",
  GEN_UNKNOWN.age_at_last_vital_status::integer as "age_at_last_vital_status",
  GEN_UNKNOWN.vital_status::text as "vital_status",
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

