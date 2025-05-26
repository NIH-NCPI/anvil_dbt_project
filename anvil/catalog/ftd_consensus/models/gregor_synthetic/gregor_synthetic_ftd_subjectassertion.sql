{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        GEN_UNKNOWN.assertion_type::text as "assertion_type",
       GEN_UNKNOWN.age_at_assertion::text as "age_at_assertion",
       GEN_UNKNOWN.age_at_event::text as "age_at_event",
       GEN_UNKNOWN.age_at_resolution::text as "age_at_resolution",
       GEN_UNKNOWN.code::text as "code",
       GEN_UNKNOWN.display::text as "display",
       GEN_UNKNOWN.value_code::text as "value_code",
       GEN_UNKNOWN.value_display::text as "value_display",
       GEN_UNKNOWN.value_number::text as "value_number",
       GEN_UNKNOWN.value_units::text as "value_units",
       GEN_UNKNOWN.value_units_display::text as "value_units_display",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='sa',descriptor=['participant.anvil_gregor_gss_u07_gru_participant_id'], study_id='gregor_synthetic') }}::text as "id",
       {{ generate_global_id(prefix='sb',descriptor=['phenotype.participant_id'], study_id='gregor_synthetic') }}::text as "Subject_id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
on participant.anvil_gregor_gss_u07_gru_participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    