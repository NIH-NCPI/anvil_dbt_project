{{ config(materialized='table', schema='alscompute_data') }}

select 
NULL::text as "demographics_id",
NULL::text as "source_data_id"
