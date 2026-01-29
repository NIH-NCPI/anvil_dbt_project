{{ config(materialized='table', schema='alscompute_data') }}

select 
NULL::text as "file_id",
NULL::text as "subject_id"
