{{ config(materialized='table', schema='alscompute_data') }}

select 
NULL::text as "datasource_id",
NULL::text as "external_id"