{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
  NULL::text as "family_id",
  NULL::text as "family_relationships_id"