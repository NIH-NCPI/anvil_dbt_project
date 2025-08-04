{{ config(materialized='table', schema='cmg_bh_data') }}

select 
--   {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "aliquot_id",
    NULL::text as "aliquot_id",
    NULL::text as "external_id"
from unique_ids