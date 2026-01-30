{{ config(materialized='table', schema='alscompute_data') }}

with sm_files as (
    select 
    cram as file,
    sample_id
    from {{ ref('alscompute_stg_sample') }}
    where cram is not null
    
    union all
    
    select
    cram_index as file,
    sample_id
    from {{ ref('alscompute_stg_sample') }}
    where cram_index is not null

    
    union all
    
    select
    cram_md5sum as file,
    sample_id
    from {{ ref('alscompute_stg_sample') }}
    where cram_md5sum is not null

    union all
    
    select
    gvcf as file,
    sample_id
    from {{ ref('alscompute_stg_sample') }}
    where gvcf is not null

    
    union all
    
    select
    gvcf_index as file,
    sample_id
    from {{ ref('alscompute_stg_sample') }}    
    where gvcf_index is not null
    
    union all
    
    select
    targeted_expansion_hunter_vcf as file,
    sample_id
    from {{ ref('alscompute_stg_sample') }}    
    where targeted_expansion_hunter_vcf is not null
    )

select distinct
  {{ generate_global_id(prefix='fi',descriptor=['f.name'], study_id='alscompute') }}::text as "file_id",
    {{ generate_global_id(prefix='sm',descriptor=['sf.sample_id'], study_id='alscompute') }}::text as "sample_id"
from {{ ref('alscompute_stg_file_inventory') }} as f
left join sm_files as sf
on sf.file = f.file_ref
