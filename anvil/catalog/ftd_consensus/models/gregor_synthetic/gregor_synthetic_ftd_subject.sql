{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        GEN_UNKNOWN.subject_type::text as "subject_type",
       GEN_UNKNOWN.organism_type::text as "organism_type",
       GEN_UNKNOWN.has_access_policy::text as "has_access_policy",
       participant.anvil_gregor_gss_u07_gru_participant_id::text as "id",
       GEN_UNKNOWN.has_demographics_id::text as "has_demographics_id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype using (ftd_key)
    )

    select 
        * 
    from source
    