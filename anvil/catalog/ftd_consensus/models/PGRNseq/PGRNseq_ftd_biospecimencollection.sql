{{ config(materialized='table', schema='PGRNseq_data') }}

select 
GEN_UNKNOWN.age_at_collection::integer as "age_at_collection",
  GEN_UNKNOWN.method::text as "method",
  GEN_UNKNOWN.site::text as "site",
  GEN_UNKNOWN.spatial_qualifier::text as "spatial_qualifier",
  GEN_UNKNOWN.laterality::text as "laterality",
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

