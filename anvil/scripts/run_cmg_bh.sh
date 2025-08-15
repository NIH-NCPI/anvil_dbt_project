#!/bin/bash
# +
dbt clean
dbt deps || { echo "Error: dbt deps failed. Exiting..."; exit 1; }
dbt seed #--full-refresh

dbt run --select  cmg_bh_stg_subject
dbt run --select  cmg_bh_stg_sample

dbt run --select  cmg_bh_ftd_accesspolicy
dbt run --select  cmg_bh_ftd_accesspolicy_access_policy_code
dbt run --select  cmg_bh_ftd_accesspolicy_external_id
# dbt run --select  cmg_bh_ftd_biospecimencollection
# dbt run --select  cmg_bh_ftd_biospecimencollection_external_id
dbt run --select  cmg_bh_ftd_datasource
dbt run --select  cmg_bh_ftd_datasource_external_id
dbt run --select  cmg_bh_ftd_demographics
dbt run --select  cmg_bh_ftd_demographics_external_id
dbt run --select  cmg_bh_ftd_demographics_race
dbt run --select  cmg_bh_ftd_demographics_source_data
dbt run --select  cmg_bh_ftd_family
dbt run --select  cmg_bh_ftd_family_external_id

dbt run --select  cmg_bh_ftd_familymember
dbt run --select  cmg_bh_ftd_familymember_external_id
dbt run --select  cmg_bh_ftd_familyrelationship
dbt run --select  cmg_bh_ftd_family_family_relationships

dbt run --select  cmg_bh_ftd_familyrelationship_external_id
dbt run --select  cmg_bh_ftd_file
dbt run --select  cmg_bh_ftd_file_external_id
dbt run --select  cmg_bh_ftd_file_sample
dbt run --select  cmg_bh_ftd_file_subject
# dbt run --select  cmg_bh_ftd_filemetadata
# dbt run --select  cmg_bh_ftd_filemetadata_external_id
dbt run --select  cmg_bh_ftd_sample
dbt run --select  cmg_bh_ftd_sample_external_id
# dbt run --select  cmg_bh_ftd_sample_processing
# dbt run --select  cmg_bh_ftd_sample_storage_method
dbt run --select  cmg_bh_ftd_sourcedata
dbt run --select  cmg_bh_ftd_sourcedata_external_id
dbt run --select  cmg_bh_ftd_study
dbt run --select  cmg_bh_ftd_study_external_study_id
dbt run --select  cmg_bh_ftd_study_external_id
dbt run --select  cmg_bh_ftd_study_funding_source
dbt run --select  cmg_bh_ftd_study_principal_investigator
dbt run --select  cmg_bh_ftd_study_funding_source
dbt run --select  cmg_bh_ftd_subject
dbt run --select  cmg_bh_ftd_subject_external_id
dbt run --select  cmg_bh_ftd_subjectassertion
dbt run --select  cmg_bh_ftd_subjectassertion_external_id
dbt run --select  cmg_bh_ftd_subjectassertion_source_data


dbt test --select cmg_bh_ftd_file_subject
dbt test --select cmg_bh_ftd_familyrelationship
dbt test --select cmg_bh_ftd_family_external_id
dbt test --select cmg_bh_ftd_sample
dbt test --select cmg_bh_ftd_study_funding_source
dbt test --select cmg_bh_ftd_file_sample
dbt test --select cmg_bh_ftd_demographics
dbt test --select cmg_bh_ftd_familyrelationship_external_id
dbt test --select cmg_bh_ftd_accesspolicy_access_policy_code
dbt test --select cmg_bh_ftd_file
dbt test --select cmg_bh_ftd_sample_storage_method
dbt test --select cmg_bh_ftd_file_external_id
dbt test --select cmg_bh_ftd_thing_external_id
dbt test --select cmg_bh_ftd_family_family_relationships
dbt test --select cmg_bh_ftd_subjectassertion
# dbt test --select cmg_bh_ftd_biospecimencollection_external_id
dbt test --select cmg_bh_ftd_family
dbt test --select cmg_bh_ftd_study_external_id
# dbt test --select cmg_bh_ftd_biospecimencollection
# dbt test --select cmg_bh_ftd_filemetadata
# dbt test --select cmg_bh_ftd_accesscontrolledrecord
# dbt test --select cmg_bh_ftd_accesscontrolledrecord_external_id
dbt test --select cmg_bh_ftd_study
dbt test --select cmg_bh_ftd_subject
dbt test --select cmg_bh_ftd_demographics_source_data
dbt test --select cmg_bh_ftd_subject_external_id
dbt test --select cmg_bh_ftd_subjectassertion
dbt test --select cmg_bh_ftd_subjectassertion_external_id
# dbt test --select cmg_bh_ftd_aliquot_external_id
dbt test --select cmg_bh_ftd_demographics_race
dbt test --select cmg_bh_ftd_sample_external_id
dbt test --select cmg_bh_ftd_sample_processing
dbt test --select cmg_bh_ftd_study_principal_investigator
# dbt test --select cmg_bh_ftd_filemetadata_external_id
dbt test --select cmg_bh_ftd_accesspolicy_data_access_type
dbt test --select cmg_bh_ftd_subjectassertion_source_data
dbt test --select cmg_bh_ftd_accesspolicy
dbt test --select cmg_bh_ftd_familymember
dbt test --select cmg_bh_ftd_accesspolicy_external_id
dbt test --select cmg_bh_ftd_sourcedata_external_id
dbt test --select cmg_bh_ftd_study_external_study_id
dbt test --select cmg_bh_ftd_familymember_external_id
# dbt test --select cmg_bh_ftd_aliquot
dbt test --select cmg_bh_ftd_demographics_external_id
dbt test --select cmg_bh_ftd_sourcedata
