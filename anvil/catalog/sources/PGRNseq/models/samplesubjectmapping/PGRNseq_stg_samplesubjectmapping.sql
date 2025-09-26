{{ config(materialized='table') }}

with source as (
    select 
      "subject_id"::text as "subject_id",
       "sample_id"::text as "sample_id",
       "sample_source"::text as "sample_source",
       "source_sample_id"::text as "source_sample_id",
       "batch"::text as "batch",
       "sample_use"::text as "sample_use",
       "sequencing_center"::text as "sequencing_center"
    from {{ source('PGRNseq','PGRNseq_SampleSubjectMapping_DS_20171120') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source
