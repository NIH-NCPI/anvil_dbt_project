{{ config(materialized='table', schema='alscompute_data') }}

select 
  NULL::text as "has_access_policy",
  NULL::text as "id"