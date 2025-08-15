{{ config(materialized='table') }}

    with source as (
        select 
        "subject_id"::text as "subject_id",
       "PHE_401.1"::text as "phe_401.1",
       "PHE_250.2"::text as "phe_250.2",
       "PHE_272.1"::text as "phe_272.1",
       "PHE_411.4"::text as "phe_411.4",
       "PHE_495"::text as "phe_495",
       "PHE_427.21"::text as "phe_427.21",
       "PHE_530.11"::text as "phe_530.11",
       "PHE_296.22"::text as "phe_296.22",
       "PHE_313.1"::text as "phe_313.1",
       "PHE_244.4"::text as "phe_244.4",
       "PHE_476"::text as "phe_476",
       "PHE_272.11"::text as "phe_272.11",
       "PHE_278.1"::text as "phe_278.1",
       "PHE_174.11"::text as "phe_174.11",
       "PHE_296.2"::text as "phe_296.2",
       "PHE_306"::text as "phe_306",
       "PHE_327.32"::text as "phe_327.32",
       "PHE_313.3"::text as "phe_313.3",
       "PHE_278.11"::text as "phe_278.11",
       "PHE_272.13"::text as "phe_272.13",
       "PHE_587"::text as "phe_587"
        from {{ source('eMERGEseq','eMERGEseq_Phecode_DS_20200925') }}
    )

    select 
        ROW_NUMBER() OVER () AS ftd_index
        ,source.*
        from source
    