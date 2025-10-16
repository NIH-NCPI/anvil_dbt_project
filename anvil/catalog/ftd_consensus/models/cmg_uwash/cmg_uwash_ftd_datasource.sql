{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
ds.snapshot_id::text as "snapshot_id",
ds.google_data_project::text as "google_data_project",
ds.snapshot_dataset::text as "snapshot_dataset",
NULL::text as "table_id",
NULL::text as "parameterized_query",
{{ generate_global_id(prefix='ds',descriptor=['ad.dataset_id'], study_id='phs000693') }}::text as "id"
from {{ ref('cmg_uwash_stg_anvil_dataset') }} as ad
left join {{ ref('ad_data_source') }} as ds
using(consent_group)