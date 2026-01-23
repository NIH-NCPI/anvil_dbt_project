{{ config(materialized='table') }}

with source as (
    select 
      "harmonized_genotypes_id"::text as "harmonized_genotypes_id",
       "annotated_harmonized_vcf"::text as "annotated_harmonized_vcf",
       "annotated_harmonized_vcf_index"::text as "annotated_harmonized_vcf_index",
       "harmonized_vcf"::text as "harmonized_vcf",
       "harmonized_vcf_index"::text as "harmonized_vcf_index"
    from {{ source('alscompute','alscompute_hmb_harmonized_genotypes') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source
