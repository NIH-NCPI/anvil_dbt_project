{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        GEN_UNKNOWN.filename::text as "filename",
       GEN_UNKNOWN.format::text as "format",
       GEN_UNKNOWN.data_type::text as "data_type",
       GEN_UNKNOWN.size::integer as "size",
       GEN_UNKNOWN.drs_uri::text as "drs_uri",
       GEN_UNKNOWN.file_metadata::text as "file_metadata",
       GEN_UNKNOWN.has_access_policy::text as "has_access_policy",
       GEN_UNKNOWN.id::text as "id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype using (ftd_key)
    )

    select 
        * 
    from source
    