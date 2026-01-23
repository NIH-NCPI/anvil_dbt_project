{{ config(materialized='table', schema='alscompute_data') }}

select 
GEN_UNKNOWN.date_of_birth::integer as "date_of_birth",
  GEN_UNKNOWN.date_of_birth_type::text as "date_of_birth_type",
  GEN_UNKNOWN.sex::text as "sex",
  GEN_UNKNOWN.sex_display::text as "sex_display",
  GEN_UNKNOWN.race_display::text as "race_display",
  GEN_UNKNOWN.ethnicity::text as "ethnicity",
  GEN_UNKNOWN.ethnicity_display::text as "ethnicity_display",
  GEN_UNKNOWN.age_at_last_vital_status::integer as "age_at_last_vital_status",
  GEN_UNKNOWN.vital_status::text as "vital_status",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='alscompute') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='alscompute') }}::text as "id"
from {{ ref('alscompute_stg_anvil_dataset') }} as anvil_dataset
join {{ ref('alscompute_stg_file_inventory') }} as file_inventory
on harmonized_genotypes.file_inventory_id = file_inventory.file_inventory_id  join {{ ref('alscompute_stg_harmonized_genotypes') }} as harmonized_genotypes
on file_inventory.file_inventory_id = harmonized_genotypes.file_inventory_id  join {{ ref('alscompute_stg_sample') }} as sample
on  

