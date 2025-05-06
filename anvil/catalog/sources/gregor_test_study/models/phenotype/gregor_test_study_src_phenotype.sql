{{ config(materialized='table') }}

select * from gregor_test_study_src_data.phenotype
