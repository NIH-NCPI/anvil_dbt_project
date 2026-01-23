{{ config(materialized='table', schema='alscompute_data') }}

select 
  {{ generate_global_id(prefix='',descriptor=[''], study_id='alscompute') }}::text as "parent_sample",
  GEN_UNKNOWN.sample_type::text as "sample_type",
  GEN_UNKNOWN.availablity_status::text as "availablity_status",
  GEN_UNKNOWN.quantity_number::text as "quantity_number",
  GEN_UNKNOWN.quantity_units::text as "quantity_units",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='alscompute') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='alscompute') }}::text as "id",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='alscompute') }}::text as "subject_id",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='alscompute') }}::text as "biospecimen_collection_id"
from {{ ref('alscompute_stg_anvil_dataset') }} as anvil_dataset
join {{ ref('alscompute_stg_file_inventory') }} as file_inventory
on harmonized_genotypes.file_inventory_id = file_inventory.file_inventory_id  join {{ ref('alscompute_stg_harmonized_genotypes') }} as harmonized_genotypes
on file_inventory.file_inventory_id = harmonized_genotypes.file_inventory_id  join {{ ref('alscompute_stg_sample') }} as sample
on  

