{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
  NULL::text as "has_access_policy",
  NULL::text as "id"