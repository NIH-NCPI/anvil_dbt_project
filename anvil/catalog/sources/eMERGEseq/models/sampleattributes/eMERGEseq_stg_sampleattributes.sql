{{ config(materialized='table') }}

    with source as (
        select 
        "sample_id"::text as "sample_id",
       "analyte_type"::text as "analyte_type",
       "sequencing_center"::text as "sequencing_center"
        from {{ source('eMERGEseq','eMERGEseq_SampleAttributes_DS_20200925') }}
    )

    select 
        ROW_NUMBER() OVER () AS ftd_index
        ,source.*
        from source
    