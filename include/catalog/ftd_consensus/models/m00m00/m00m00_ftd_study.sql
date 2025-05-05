{{ config(materialized='table', schema='m00m00_data') }}

with source as (
    select 
       participant.study_code::text AS "study_code",
       GEN_UNKNOWN.study_title::text AS "study_title",
       GEN_UNKNOWN.program::text AS "program",
       GEN_UNKNOWN.study_description::text AS "study_description",
       GEN_UNKNOWN.principal_investigator_name::text AS "principal_investigator_name",
       GEN_UNKNOWN.study_contact_name::text AS "study_contact_name",
       GEN_UNKNOWN.study_contact_institution::text AS "study_contact_institution",
       GEN_UNKNOWN.study_contact_email::text AS "study_contact_email",
       GEN_UNKNOWN.vbr_email::text AS "vbr_email",
       GEN_UNKNOWN.vbr_url::text AS "vbr_url",
       GEN_UNKNOWN.vbr_readme::text AS "vbr_readme",
       GEN_UNKNOWN.research_domain::text AS "research_domain",
       GEN_UNKNOWN.participant_lifespan_stage::text AS "participant_lifespan_stage",
       GEN_UNKNOWN.selection_criteria::text AS "selection_criteria",
       GEN_UNKNOWN.study_design::text AS "study_design",
       GEN_UNKNOWN.clinical_data_source_type::text AS "clinical_data_source_type",
       GEN_UNKNOWN.data_category::text AS "data_category",
       GEN_UNKNOWN.study_website::text AS "study_website",
       GEN_UNKNOWN.dbgap::text AS "dbgap",
       GEN_UNKNOWN.publication::text AS "publication",
       GEN_UNKNOWN.expected_number_of_participants::text AS "expected_number_of_participants",
       GEN_UNKNOWN.guid_type::text AS "guid_type",
       GEN_UNKNOWN.guid_mapped::text AS "guid_mapped",
       GEN_UNKNOWN.acknowledgments::text AS "acknowledgments",
       GEN_UNKNOWN.citation_statement::text AS "citation_statement"
    from {{ ref('m00m00_stg_participant') }} AS participant
    JOIN {{ ref('m00m00_stg_condition') }} AS condition USING (ftd_key)
)

select 
    * 
from source
