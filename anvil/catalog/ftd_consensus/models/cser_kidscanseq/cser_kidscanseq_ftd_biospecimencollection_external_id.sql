{{ config(materialized='table', schema='cser_kidscanseq_data') }}

select 
NULL::text as "biospecimencollection_id",
NULL::text as "external_id"