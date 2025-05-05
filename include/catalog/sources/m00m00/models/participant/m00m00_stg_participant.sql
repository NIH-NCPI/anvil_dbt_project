{{ config(materialized='table') }}

with source as (
    select 
       "Study Code"::text AS "study_code",
       "Participant Global ID"::text AS "participant_global_id",
       "Participant External ID"::text AS "participant_external_id",
       "Event ID"::integer AS "event_id",
       "Event Type"::text AS "event_type",
       "Condition or Measure Source Text"::text AS "condition_or_measure_source_text",
       "Age at Condition or Measure Observation"::text AS "age_at_condition_or_measure_observation",
       "Condition Interpretation"::text AS "condition_interpretation",
       "Condition Status"::text AS "condition_status",
       "Condition Data Source"::text AS "condition_data_source",
       "HPO Label"::text AS "hpo_label",
       "HPO Code"::text AS "hpo_code",
       "MONDO Label"::text AS "mondo_label",
       "MONDO Code"::text AS "mondo_code",
       "MAXO Label"::text AS "maxo_label",
       "MAXO Code"::text AS "maxo_code",
       "Other Label"::text AS "other_label",
       "Other Code"::text AS "other_code",
       "Measure Value"::float AS "measure_value",
       "Measure Unit"::text AS "measure_unit"
    from {{ ref('m00m00_src_participant') }}
)

select 
    *,
    concat(study_code, '-', participant_global_id) AS ftd_key
from source
