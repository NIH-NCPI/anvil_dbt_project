{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  NULL::text as "availablity_status",
  NULL::text as "quantity_number",
  NULL::text as "quantity_units",
  NULL::text as "concentration_number",
  NULL::text as "concentration_unit",
  NULL::text as "has_access_policy",
  NULL::text as "id",
  NULL::text as "sample_id"
--   { { generate_global_id(prefix='ap',descriptor=['ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
--   { { generate_global_id(prefix='aq',descriptor=[''], study_id='cmg_bh') }}::text as "id",
--   { { generate_global_id(prefix='sm',descriptor=['sample_id'], study_id='cmg_bh') }}::text as "sample_id"
from {{ ref('cmg_bh_stg_sample') }} as sample

    