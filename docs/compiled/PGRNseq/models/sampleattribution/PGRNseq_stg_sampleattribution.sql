

with source as (
    select 
      "sample_id"::text as "sample_id",
       "analyte_type"::text as "analyte_type"
    from "dbt"."main"."PGRNseq_SampleAttribution_DS_20171120"
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source