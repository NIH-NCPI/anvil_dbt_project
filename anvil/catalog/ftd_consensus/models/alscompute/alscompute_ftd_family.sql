{{ config(materialized='table', schema='alscompute_data') }}

select 
GEN_UNKNOWN.family_type::text as "family_type",
  GEN_UNKNOWN.family_description::text as "family_description",
  GEN_UNKNOWN.consanguinity::text as "consanguinity",
  GEN_UNKNOWN.family_study_focus::text as "family_study_focus",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='alscompute') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='alscompute') }}::text as "id"
from {{ ref('alscompute_stg_anvil_dataset') }} as anvil_dataset
join {{ ref('alscompute_stg_file_inventory') }} as file_inventory
on harmonized_genotypes.file_inventory_id = file_inventory.file_inventory_id  join {{ ref('alscompute_stg_harmonized_genotypes') }} as harmonized_genotypes
on file_inventory.file_inventory_id = harmonized_genotypes.file_inventory_id  join {{ ref('alscompute_stg_sample') }} as sample
on  

