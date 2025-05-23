{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        participant.ftd_key,
        'human'::text as "subject_type",
        'mammal'::text as "organism_type",
    --    GEN_UNKNOWN.has_access_policy::text as "has_access_policy",
       {{ generate_uuid_key('s') }}::text as "id",
    --    GEN_UNKNOWN.has_demographics_id::text as "has_demographics_id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
    ),

    
    get_ap_fk as (
        select 
        "id",
        "ftd_key"
        from {{ ref('gregor_synthetic_ftd_accesspolicy') }}
    ),


    get_d_fk as (
        select 
        "id",
        "ftd_key"
        from {{ ref('gregor_synthetic_ftd_demographics') }}
    )



    select 
        source.*,
        ap.id::text as "has_access_policy",
        d.id::text as "has_demographics_id"
    from source
    join get_ap_fk as ap using (ftd_key)
    join get_d_fk as d using (ftd_key)
    