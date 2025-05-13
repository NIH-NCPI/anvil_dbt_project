{{ config(materialized='table', schema='gregor_ts_data') }}

    with source as (
        select 
        GEN_UNKNOWN.study_code::text AS "study_code",
       GEN_UNKNOWN.participant_global_id::text AS "participant_global_id",
       GEN_UNKNOWN.participant_external_id::text AS "participant_external_id",
       GEN_UNKNOWN.event_id::text AS "event_id",
       GEN_UNKNOWN.event_type::text AS "event_type",
       GEN_UNKNOWN.condition_or_measure_source_text::text AS "condition_or_measure_source_text",
       GEN_UNKNOWN.age_at_condition_or_measure_observation::integer AS "age_at_condition_or_measure_observation",
       GEN_UNKNOWN.condition_interpretation::text AS "condition_interpretation",
       GEN_UNKNOWN.condition_status::text AS "condition_status",
       GEN_UNKNOWN.condition_data_source::text AS "condition_data_source",
       GEN_UNKNOWN.hpo_label::text AS "hpo_label",
       GEN_UNKNOWN.hpo_code::text AS "hpo_code",
       GEN_UNKNOWN.mondo_label::text AS "mondo_label",
       GEN_UNKNOWN.mondo_code::text AS "mondo_code",
       GEN_UNKNOWN.maxo_label::text AS "maxo_label",
       GEN_UNKNOWN.maxo_code::text AS "maxo_code",
       GEN_UNKNOWN.other_label::text AS "other_label",
       GEN_UNKNOWN.other_code::text AS "other_code",
       GEN_UNKNOWN.measure_value::integer AS "measure_value",
       GEN_UNKNOWN.measure_unit::text AS "measure_unit"
        from {{ ref('gregor_ts_stg_participant') }} as participant
        join {{ ref('gregor_ts_stg_phenotype') }} AS phenotype using (ftd_key)
    )

    select 
        * 
    from source
    