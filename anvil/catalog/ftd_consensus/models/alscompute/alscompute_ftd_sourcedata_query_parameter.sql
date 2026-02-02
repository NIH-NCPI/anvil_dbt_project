{{ config(materialized='table', schema='alscompute_data') }}

select 
  NULL::text as "sourcedata_id",
