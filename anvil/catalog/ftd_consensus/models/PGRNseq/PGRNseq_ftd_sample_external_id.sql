{{ config(materialized='table', schema='PGRNseq_data') }}

select distinct
  {{ generate_global_id(prefix='sm',descriptor=['samplesubjectmapping.subject_id', 'samplesubjectmapping.sample_id', 'subjectconsent.consent'], study_id='phs000906') }}::text as "sample_id",
  samplesubjectmapping.sample_id::text as "external_id"
from {{ ref('PGRNseq_stg_samplesubjectmapping') }} as samplesubjectmapping

