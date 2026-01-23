{{ config(materialized='table') }}

with source as (
    select 
      "sample_id"::text as "sample_id",
       "c9orf72_repeat_expansion_imputed"::text as "c9orf72_repeat_expansion_imputed",
       "c9orf72_repeat_length"::text as "c9orf72_repeat_length",
       "affected_status"::text as "affected_status",
       "age_at_death_years"::text as "age_at_death_years",
       "age_of_collection_years"::text as "age_of_collection_years",
       "age_of_diagnosis_years"::text as "age_of_diagnosis_years",
       "age_of_onset_years"::text as "age_of_onset_years",
       "concordant_gender"::text as "concordant_gender",
       "cram"::text as "cram",
       "cram_index"::text as "cram_index",
       "cram_md5sum"::text as "cram_md5sum",
       "diagnosis"::text as "diagnosis",
       "diagnosis_subtype"::text as "diagnosis_subtype",
       "family_history"::text as "family_history",
       "gvcf"::text as "gvcf",
       "gvcf_index"::text as "gvcf_index",
       "imputed_gender_text"::text as "imputed_gender_text",
       "reference_genome_build"::text as "reference_genome_build",
       "reported_gender_coded"::text as "reported_gender_coded",
       "reported_gender_text"::text as "reported_gender_text",
       "sequencing_id"::text as "sequencing_id",
       "sequencing_platform"::text as "sequencing_platform",
       "site_of_onset"::text as "site_of_onset",
       "survival_months"::text as "survival_months",
       "survival_years"::text as "survival_years",
       "targeted_expansion_hunter_vcf"::text as "targeted_expansion_hunter_vcf"
    from {{ source('alscompute','alscompute_hmb_sample') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*,
  (select REPLACE(UPPER(title),'ANVIL_ALSCompute_Collection_','') from {{ source('alscompute','alscompute_hmb_anvil_dataset') }}
    limit 1
  ) as consent_id
  from source
