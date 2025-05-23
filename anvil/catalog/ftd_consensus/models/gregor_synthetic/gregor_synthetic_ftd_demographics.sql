{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        participant.ftd_key,
    --    GEN_UNKNOWN.date_of_birth::integer as "date_of_birth",
    --    GEN_UNKNOWN.date_of_birth_type::text as "date_of_birth_type",
    --    participant.sex::text as "sex",
    --    GEN_UNKNOWN.sex_display::text as "sex_display",
    --    participant.reported_race::text as "race_display",
    --    GEN_UNKNOWN.ethnicity::text as "ethnicity",
    --    GEN_UNKNOWN.ethnicity_display::text as "ethnicity_display",
    --    GEN_UNKNOWN.age_at_last_vital_status::integer as "age_at_last_vital_status",
    --    GEN_UNKNOWN.vital_status::text as "vital_status",
    --    GEN_UNKNOWN.has_access_policy::text as "has_access_policy",
       {{ generate_md5_composite_key(prefix='d',columns=['participant.anvil_gregor_gss_u07_gru_participant_id','participant.ftd_key']) }}::text as "id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
    )

    select 
        * 
    from source
    