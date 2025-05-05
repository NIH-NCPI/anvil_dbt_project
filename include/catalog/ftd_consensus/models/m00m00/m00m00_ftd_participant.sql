{{ config(materialized='table', schema='m00m00_data') }}

with source as (
    select 
       participant.study_code::text AS "study_code",
       participant.participant_global_id::text AS "participant_global_id",
       participant.participant_external_id::text AS "participant_external_id",
       GEN_UNKNOWN.family_id::text AS "family_id",
       GEN_UNKNOWN.family_type::text AS "family_type",
       GEN_UNKNOWN.father_id::text AS "father_id",
       GEN_UNKNOWN.mother_id::text AS "mother_id",
       GEN_UNKNOWN.sibling_id::text AS "sibling_id",
       GEN_UNKNOWN.other_family_member_id::text AS "other_family_member_id",
       GEN_UNKNOWN.family_relationship::text AS "family_relationship",
       GEN_UNKNOWN.sex::text AS "sex",
       GEN_UNKNOWN.race::text AS "race",
       GEN_UNKNOWN.ethnicity::text AS "ethnicity",
       GEN_UNKNOWN.down_syndrome_status::text AS "down_syndrome_status",
       GEN_UNKNOWN.age_at_first_patient_engagement::integer AS "age_at_first_patient_engagement",
       GEN_UNKNOWN.first_patient_engagement_event::text AS "first_patient_engagement_event",
       GEN_UNKNOWN.outcomes_vital_status::text AS "outcomes_vital_status",
       GEN_UNKNOWN.age_at_last_vital_status::integer AS "age_at_last_vital_status"
    from {{ ref('m00m00_stg_participant') }} AS participant
    JOIN {{ ref('m00m00_stg_condition') }} AS condition USING (ftd_key)
)

select 
    * 
from source
