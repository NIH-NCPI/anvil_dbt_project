{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
NULL::text as "biospecimencollection_id",
NULL::text as "external_id"