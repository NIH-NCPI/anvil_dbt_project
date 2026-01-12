{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
  NULL::text as "accesscontrolledrecord_id",
  NULL::text as "external_id"