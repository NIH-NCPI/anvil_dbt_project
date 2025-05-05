{{ config(materialized='table', schema='m00m00_data') }}

with source as (
    select 
       participant.study_code::text AS "study_code",
       participant.participant_global_id::text AS "participant_global_id",
       participant.participant_external_id::text AS "participant_external_id",
       participant.event_id::text AS "event_id",
       participant.event_type::text AS "event_type",
       participant.condition_or_measure_source_text::text AS "condition_or_measure_source_text",
       participant.age_at_condition_or_measure_observation::integer AS "age_at_condition_or_measure_observation",
       participant.condition_interpretation::text AS "condition_interpretation",
       participant.condition_status::text AS "condition_status",
       participant.condition_data_source::text AS "condition_data_source",
       participant.hpo_label::text AS "hpo_label",
       participant.hpo_code::text AS "hpo_code",
       participant.mondo_label::text AS "mondo_label",
       participant.mondo_code::text AS "mondo_code",
       participant.maxo_label::text AS "maxo_label",
       participant.maxo_code::text AS "maxo_code",
       participant.other_label::text AS "other_label",
       participant.other_code::text AS "other_code",
       participant.measure_value::integer AS "measure_value",
       participant.measure_unit::text AS "measure_unit"
    from {{ ref('m00m00_stg_participant') }} AS participant
    JOIN {{ ref('m00m00_stg_condition') }} AS condition USING (ftd_key)
)

select 
    * 
from source
