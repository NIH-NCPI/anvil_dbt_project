{{ config(materialized='table') }}

    with source as (
        select 
        "subject_id"::text as "subject_id",
       "sex"::text as "sex",
       "decade_birth"::text as "decade_birth",
       "year_birth"::text as "year_birth",
       "ethnicity"::text as "ethnicity",
       "race"::text as "race"
        from {{ source('eMERGEseq','eMERGEseq_Demographics_DS_20200925') }}
    )

    select 
        ROW_NUMBER() OVER () AS ftd_index
        ,source.*
        from source
    