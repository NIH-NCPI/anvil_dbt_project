{{ config(materialized='table', schema='alscompute_data') }}

select 
GEN_UNKNOWN.snapshot_id::text as "snapshot_id",
  GEN_UNKNOWN.google_data_project::text as "google_data_project",
  GEN_UNKNOWN.snapshot_dataset::text as "snapshot_dataset",
  GEN_UNKNOWN.table_id::text as "table_id",
  GEN_UNKNOWN.parameterized_query::text as "parameterized_query",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='alscompute') }}::text as "id"
from {{ ref('alscompute_stg_anvil_dataset') }} as anvil_dataset
join {{ ref('alscompute_stg_file_inventory') }} as file_inventory
on harmonized_genotypes.file_inventory_id = file_inventory.file_inventory_id  join {{ ref('alscompute_stg_harmonized_genotypes') }} as harmonized_genotypes
on file_inventory.file_inventory_id = harmonized_genotypes.file_inventory_id  join {{ ref('alscompute_stg_sample') }} as sample
on  

