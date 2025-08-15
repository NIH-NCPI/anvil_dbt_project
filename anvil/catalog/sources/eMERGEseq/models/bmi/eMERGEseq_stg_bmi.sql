{{ config(materialized='table') }}

    with source as (
        select 
        "subject_id"::text as "subject_id",
       "age_at_observation"::text as "age_at_observation",
       "weight"::text as "weight",
       "height"::text as "height",
       "body_mass_index"::text as "body_mass_index",
       "visit_number"::text as "visit_number"
        from {{ source('eMERGEseq','eMERGEseq_BMI_DS_20200925') }}
    )

    select 
        ROW_NUMBER() OVER () AS ftd_index
        ,source.*
        from source
    