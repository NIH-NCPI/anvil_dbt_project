{{ config(materialized='table') }}

with source as (
    select 
      "sample_id"::text as "sample_id",
       "analyte_type"::text as "analyte_type"
    from {{ source('GWAS','GWAS_SampleAttributes_DS_20201027') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source
