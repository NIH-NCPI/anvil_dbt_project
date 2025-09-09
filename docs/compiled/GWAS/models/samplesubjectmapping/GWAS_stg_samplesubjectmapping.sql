

with source as (
    select 
      "subject_id"::text as "subject_id",
       "sample_id"::text as "sample_id",
       "samp_source"::text as "samp_source",
       "source_sampid"::text as "source_sampid"
    from "dbt"."main"."GWAS_SampleSubjectMapping_DS_20201027"
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source