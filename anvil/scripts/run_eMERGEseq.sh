#!/bin/bash
set -e  # Exit on error
dbt clean
dbt deps || { echo "Error: dbt deps failed. Exiting..."; exit 1; }
# Run Target tables
dbt run --select +tgt_sample --vars '{"source_table": "eMERGEseq_ftd_sample", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_demographics_source_data --vars '{"source_table": "eMERGEseq_ftd_demographics_source_data", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_family --vars '{"source_table": "eMERGEseq_ftd_family", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_accesscontrolledrecord --vars '{"source_table": "eMERGEseq_ftd_accesscontrolledrecord", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_biospecimencollection_external_id --vars '{"source_table": "eMERGEseq_ftd_biospecimencollection_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_subjectassertion --vars '{"source_table": "eMERGEseq_ftd_subjectassertion", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_accesspolicy --vars '{"source_table": "eMERGEseq_ftd_accesspolicy", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_filemetadata --vars '{"source_table": "eMERGEseq_ftd_filemetadata", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_file --vars '{"source_table": "eMERGEseq_ftd_file", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_sample_processing --vars '{"source_table": "eMERGEseq_ftd_sample_processing", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_file_sample --vars '{"source_table": "eMERGEseq_ftd_file_sample", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_filemetadata_external_id --vars '{"source_table": "eMERGEseq_ftd_filemetadata_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_subject --vars '{"source_table": "eMERGEseq_ftd_subject", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_sample_storage_method --vars '{"source_table": "eMERGEseq_ftd_sample_storage_method", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_sample_external_id --vars '{"source_table": "eMERGEseq_ftd_sample_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_familyrelationship_external_id --vars '{"source_table": "eMERGEseq_ftd_familyrelationship_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_family_family_relationships --vars '{"source_table": "eMERGEseq_ftd_family_family_relationships", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_demographics_race --vars '{"source_table": "eMERGEseq_ftd_demographics_race", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_study_funding_source --vars '{"source_table": "eMERGEseq_ftd_study_funding_source", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_family_external_id --vars '{"source_table": "eMERGEseq_ftd_family_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_accesspolicy_external_id --vars '{"source_table": "eMERGEseq_ftd_accesspolicy_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_aliquot --vars '{"source_table": "eMERGEseq_ftd_aliquot", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_demographics --vars '{"source_table": "eMERGEseq_ftd_demographics", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_familymember_external_id --vars '{"source_table": "eMERGEseq_ftd_familymember_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_accesspolicy_access_policy_code --vars '{"source_table": "eMERGEseq_ftd_accesspolicy_access_policy_code", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_thing_external_id --vars '{"source_table": "eMERGEseq_ftd_thing_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_study --vars '{"source_table": "eMERGEseq_ftd_study", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_subjectassertion_external_id --vars '{"source_table": "eMERGEseq_ftd_subjectassertion_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_biospecimencollection --vars '{"source_table": "eMERGEseq_ftd_biospecimencollection", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_familyrelationship --vars '{"source_table": "eMERGEseq_ftd_familyrelationship", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_aliquot_external_id --vars '{"source_table": "eMERGEseq_ftd_aliquot_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_familymember --vars '{"source_table": "eMERGEseq_ftd_familymember", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_subject_external_id --vars '{"source_table": "eMERGEseq_ftd_subject_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_file_external_id --vars '{"source_table": "eMERGEseq_ftd_file_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_demographics_external_id --vars '{"source_table": "eMERGEseq_ftd_demographics_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_subjectassertion_source_data --vars '{"source_table": "eMERGEseq_ftd_subjectassertion_source_data", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_study_external_study_id --vars '{"source_table": "eMERGEseq_ftd_study_external_study_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_file_subject --vars '{"source_table": "eMERGEseq_ftd_file_subject", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_accesspolicy_data_access_type --vars '{"source_table": "eMERGEseq_ftd_accesspolicy_data_access_type", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_accesscontrolledrecord_external_id --vars '{"source_table": "eMERGEseq_ftd_accesscontrolledrecord_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_sourcedata --vars '{"source_table": "eMERGEseq_ftd_sourcedata", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_sourcedata_external_id --vars '{"source_table": "eMERGEseq_ftd_sourcedata_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_study_principal_investigator --vars '{"source_table": "eMERGEseq_ftd_study_principal_investigator", "target_schema": "eMERGEseq_tgt_data"}'
dbt run --select +tgt_study_external_id --vars '{"source_table": "eMERGEseq_ftd_study_external_id", "target_schema": "eMERGEseq_tgt_data"}'
dbt test --select eMERGEseq_ftd_sample
dbt test --select eMERGEseq_ftd_demographics_source_data
dbt test --select eMERGEseq_ftd_family
dbt test --select eMERGEseq_ftd_accesscontrolledrecord
dbt test --select eMERGEseq_ftd_biospecimencollection_external_id
dbt test --select eMERGEseq_ftd_subjectassertion
dbt test --select eMERGEseq_ftd_accesspolicy
dbt test --select eMERGEseq_ftd_filemetadata
dbt test --select eMERGEseq_ftd_file
dbt test --select eMERGEseq_ftd_sample_processing
dbt test --select eMERGEseq_ftd_file_sample
dbt test --select eMERGEseq_ftd_filemetadata_external_id
dbt test --select eMERGEseq_ftd_subject
dbt test --select eMERGEseq_ftd_sample_storage_method
dbt test --select eMERGEseq_ftd_sample_external_id
dbt test --select eMERGEseq_ftd_familyrelationship_external_id
dbt test --select eMERGEseq_ftd_family_family_relationships
dbt test --select eMERGEseq_ftd_demographics_race
dbt test --select eMERGEseq_ftd_study_funding_source
dbt test --select eMERGEseq_ftd_family_external_id
dbt test --select eMERGEseq_ftd_accesspolicy_external_id
dbt test --select eMERGEseq_ftd_aliquot
dbt test --select eMERGEseq_ftd_demographics
dbt test --select eMERGEseq_ftd_familymember_external_id
dbt test --select eMERGEseq_ftd_accesspolicy_access_policy_code
dbt test --select eMERGEseq_ftd_thing_external_id
dbt test --select eMERGEseq_ftd_study
dbt test --select eMERGEseq_ftd_subjectassertion_external_id
dbt test --select eMERGEseq_ftd_biospecimencollection
dbt test --select eMERGEseq_ftd_familyrelationship
dbt test --select eMERGEseq_ftd_aliquot_external_id
dbt test --select eMERGEseq_ftd_familymember
dbt test --select eMERGEseq_ftd_subject_external_id
dbt test --select eMERGEseq_ftd_file_external_id
dbt test --select eMERGEseq_ftd_demographics_external_id
dbt test --select eMERGEseq_ftd_subjectassertion_source_data
dbt test --select eMERGEseq_ftd_study_external_study_id
dbt test --select eMERGEseq_ftd_file_subject
dbt test --select eMERGEseq_ftd_accesspolicy_data_access_type
dbt test --select eMERGEseq_ftd_accesscontrolledrecord_external_id
dbt test --select eMERGEseq_ftd_sourcedata
dbt test --select eMERGEseq_ftd_sourcedata_external_id
dbt test --select eMERGEseq_ftd_study_principal_investigator
dbt test --select eMERGEseq_ftd_study_external_id
