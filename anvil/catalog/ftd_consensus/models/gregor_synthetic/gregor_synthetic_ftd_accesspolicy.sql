{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        participant.ftd_key,
    --    GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
    --    GEN_UNKNOWN.description::text as "description",
    --    GEN_UNKNOWN.website::text as "website",
       {{ generate_uuid_key('p') }}::text as "id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
    )

    select 
        * 
    from source
    