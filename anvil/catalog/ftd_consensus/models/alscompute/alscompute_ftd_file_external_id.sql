{{ config(materialized='table', schema='alscompute_data') }}

with hg_files as (
    select distinct
    annotated_harmonized_vcf as file
    from {{ ref('alscompute_stg_harmonized_genotypes') }}
    
    union all
    
    select distinct
    annotated_harmonized_vcf_index as file
    from {{ ref('alscompute_stg_harmonized_genotypes') }}
    
    union all
    
    select distinct
    harmonized_vcf_index as file
    from {{ ref('alscompute_stg_harmonized_genotypes') }}
    
    union all
    
    select distinct
    harmonized_vcf_index as file
    from {{ ref('alscompute_stg_harmonized_genotypes') }}
    )

select 
  {{ generate_global_id(prefix='fi',descriptor=['file_id'], study_id='alscompute') }}::text as "file_id",
  file_id::text as "external_id"
from (select distinct file_id, file_ref from {{ ref('alscompute_stg_file_inventory') }}) as fi
left join hg_files as hg
    on hg.file = fi.file_ref