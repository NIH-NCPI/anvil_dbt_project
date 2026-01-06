{{ config(materialized='table', schema='cser_kidscanseq_data') }}

select 
  NULL::text as "has_access_policy",
  NULL::text as "id"