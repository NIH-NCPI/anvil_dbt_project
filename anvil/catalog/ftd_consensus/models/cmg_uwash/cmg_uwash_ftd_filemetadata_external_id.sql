{{ config(materialized='table', schema='cmg_uwash_data') }}

with unioned_names as (
        select
            distinct 
            name as "filename"
        from {{ ref('cmg_uwash_stg_file_inventory') }}
    
        union all
   
        select
            distinct 
            sequencing_id as "filename"
        from {{ ref('cmg_uwash_stg_sequencing') }}
)

select 
  {{ generate_global_id(prefix='fd',descriptor=['filename'], study_id='phs000693') }}::text as "filemetadata_id",
  filename::text as "external_id"
from unioned_names as un