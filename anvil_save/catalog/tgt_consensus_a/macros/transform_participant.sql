{% macro transform_participant(source_table) %}
with source as (
    select 
       GEN_UNKNOWN.study_code::text AS "study_code",
       GEN_UNKNOWN.participant_global_id::text AS "participant_global_id",
       GEN_UNKNOWN.participant_external_id::text AS "participant_external_id",
       participant.family_id::text AS "family_id",
       GEN_UNKNOWN.family_type::text AS "Family_Type",
       GEN_UNKNOWN.down_syndrome_status::text AS "down_syndrome_status",
       GEN_UNKNOWN.age_at_first_patient_engagement::integer AS "age_at_first_patient_engagement",
       GEN_UNKNOWN.first_patient_engagement_event::text AS "first_patient_engagement_event",
       GEN_UNKNOWN.outcomes_vital_status::text AS "outcomes_vital_status",
       GEN_UNKNOWN.age_at_last_vital_status::integer AS "age_at_last_vital_status"
    from {{ ref('gregor_test_study_stg_participant') }} AS participant
    JOIN {{ ref('gregor_test_study_stg_phenotype') }} AS phenotype USING (ftd_key)
)

select 
    * 
from source
{% endmacro %}
