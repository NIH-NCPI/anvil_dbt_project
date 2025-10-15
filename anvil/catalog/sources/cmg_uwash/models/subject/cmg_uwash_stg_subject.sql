{{ config(materialized='table') }}

with source as (
    SELECT
        "affected_status"::TEXT AS "affected_status"
        ,
        NULL AS "age_at_last_observation",
        "ancestry"::TEXT AS "ancestry"
        ,
        NULL AS "ancestry_detail",
        NULL AS "congenital_status",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "dbgap_study_id",
        NULL AS "dbgap_subject_id",
        "dbgap_submission"::TEXT AS "dbgap_submission"
        ,
        NULL AS "disease_description",
        NULL AS "disease_id",
        "family_id"::TEXT AS "family_id"
        ,
        "hpo_present"::TEXT AS "hpo_present"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "maternal_id"::TEXT AS "maternal_id"
        ,
        "multiple_datasets"::TEXT AS "multiple_datasets"
        ,
        "paternal_id"::TEXT AS "paternal_id"
        ,
        "phenotype_description"::TEXT AS "phenotype_description"
        ,
        NULL AS "pmid_id",
        NULL AS "prior_testing",
        "proband_relationship"::TEXT AS "proband_relationship"
        ,
        "project_id"::TEXT AS "project_id"
        ,
        NULL AS "sample_id",
        NULL AS "sequencing_center",
        "sex"::TEXT AS "sex"
        ,
        NULL AS "solve_state",
        "subject_id"::TEXT AS "subject_id"
        ,
        NULL AS "twin_id"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/subject_ANVIL_CMG_UWash_DS_CHDEF_20250224_ANV5_202502241753_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'affected_status': 'VARCHAR'
        ,'ancestry': 'VARCHAR'
        ,'dbgap_submission': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'multiple_datasets': 'VARCHAR'
        ,'phenotype_description': 'VARCHAR'
        ,'proband_relationship': 'VARCHAR'
        ,'project_id': 'VARCHAR'
        ,'sex': 'VARCHAR'
        ,'hpo_present': 'VARCHAR'
        ,'maternal_id': 'VARCHAR'
        ,'paternal_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "affected_status"::TEXT AS "affected_status"
        ,
        NULL AS "age_at_last_observation",
        "ancestry"::TEXT AS "ancestry"
        ,
        NULL AS "ancestry_detail",
        NULL AS "congenital_status",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "dbgap_study_id",
        NULL AS "dbgap_subject_id",
        "dbgap_submission"::TEXT AS "dbgap_submission"
        ,
        NULL AS "disease_description",
        "disease_id"::TEXT AS "disease_id"
        ,
        "family_id"::TEXT AS "family_id"
        ,
        "hpo_present"::TEXT AS "hpo_present"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "maternal_id"::TEXT AS "maternal_id"
        ,
        "multiple_datasets"::TEXT AS "multiple_datasets"
        ,
        "paternal_id"::TEXT AS "paternal_id"
        ,
        "phenotype_description"::TEXT AS "phenotype_description"
        ,
        "pmid_id"::TEXT AS "pmid_id"
        ,
        NULL AS "prior_testing",
        "proband_relationship"::TEXT AS "proband_relationship"
        ,
        "project_id"::TEXT AS "project_id"
        ,
        NULL AS "sample_id",
        NULL AS "sequencing_center",
        "sex"::TEXT AS "sex"
        ,
        "solve_state"::TEXT AS "solve_state"
        ,
        "subject_id"::TEXT AS "subject_id"
        ,
        "twin_id"::TEXT AS "twin_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/subject_ANVIL_CMG_UWash_GRU_IRB_20250224_ANV5_202502241723_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'affected_status': 'VARCHAR'
        ,'ancestry': 'VARCHAR'
        ,'dbgap_submission': 'VARCHAR'
        ,'disease_id': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'hpo_present': 'VARCHAR'
        ,'maternal_id': 'VARCHAR'
        ,'multiple_datasets': 'VARCHAR'
        ,'paternal_id': 'VARCHAR'
        ,'phenotype_description': 'VARCHAR'
        ,'proband_relationship': 'VARCHAR'
        ,'project_id': 'VARCHAR'
        ,'sex': 'VARCHAR'
        ,'solve_state': 'VARCHAR'
        ,'pmid_id': 'VARCHAR'
        ,'twin_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "affected_status"::TEXT AS "affected_status"
        ,
        "age_at_last_observation"::TEXT AS "age_at_last_observation"
        ,
        "ancestry"::TEXT AS "ancestry"
        ,
        "ancestry_detail"::TEXT AS "ancestry_detail"
        ,
        "congenital_status"::TEXT AS "congenital_status"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "dbgap_study_id"::TEXT AS "dbgap_study_id"
        ,
        "dbgap_subject_id"::TEXT AS "dbgap_subject_id"
        ,
        "dbgap_submission"::TEXT AS "dbgap_submission"
        ,
        "disease_description"::TEXT AS "disease_description"
        ,
        "disease_id"::TEXT AS "disease_id"
        ,
        "family_id"::TEXT AS "family_id"
        ,
        "hpo_present"::TEXT AS "hpo_present"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "maternal_id"::TEXT AS "maternal_id"
        ,
        "multiple_datasets"::TEXT AS "multiple_datasets"
        ,
        "paternal_id"::TEXT AS "paternal_id"
        ,
        "phenotype_description"::TEXT AS "phenotype_description"
        ,
        "pmid_id"::TEXT AS "pmid_id"
        ,
        "prior_testing"::TEXT AS "prior_testing"
        ,
        "proband_relationship"::TEXT AS "proband_relationship"
        ,
        "project_id"::TEXT AS "project_id"
        ,
        NULL AS "sample_id",
        NULL AS "sequencing_center",
        "sex"::TEXT AS "sex"
        ,
        "solve_state"::TEXT AS "solve_state"
        ,
        "subject_id"::TEXT AS "subject_id"
        ,
        NULL AS "twin_id"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/subject_ANVIL_CMG_UWash_GRU_20250224_ANV5_202502241706_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'affected_status': 'VARCHAR'
        ,'dbgap_submission': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'multiple_datasets': 'VARCHAR'
        ,'phenotype_description': 'VARCHAR'
        ,'proband_relationship': 'VARCHAR'
        ,'project_id': 'VARCHAR'
        ,'sex': 'VARCHAR'
        ,'hpo_present': 'VARCHAR'
        ,'maternal_id': 'VARCHAR'
        ,'paternal_id': 'VARCHAR'
        ,'ancestry': 'VARCHAR'
        ,'disease_id': 'VARCHAR'
        ,'disease_description': 'VARCHAR'
        ,'congenital_status': 'VARCHAR'
        ,'ancestry_detail': 'VARCHAR'
        ,'dbgap_study_id': 'VARCHAR'
        ,'dbgap_subject_id': 'VARCHAR'
        ,'pmid_id': 'VARCHAR'
        ,'solve_state': 'VARCHAR'
        ,'prior_testing': 'VARCHAR'
        ,'age_at_last_observation': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "affected_status"::TEXT AS "affected_status"
        ,
        NULL AS "age_at_last_observation",
        "ancestry"::TEXT AS "ancestry"
        ,
        NULL AS "ancestry_detail",
        NULL AS "congenital_status",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "dbgap_study_id",
        NULL AS "dbgap_subject_id",
        "dbgap_submission"::TEXT AS "dbgap_submission"
        ,
        NULL AS "disease_description",
        NULL AS "disease_id",
        "family_id"::TEXT AS "family_id"
        ,
        "hpo_present"::TEXT AS "hpo_present"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "maternal_id"::TEXT AS "maternal_id"
        ,
        "multiple_datasets"::TEXT AS "multiple_datasets"
        ,
        "paternal_id"::TEXT AS "paternal_id"
        ,
        "phenotype_description"::TEXT AS "phenotype_description"
        ,
        NULL AS "pmid_id",
        NULL AS "prior_testing",
        "proband_relationship"::TEXT AS "proband_relationship"
        ,
        "project_id"::TEXT AS "project_id"
        ,
        NULL AS "sample_id",
        NULL AS "sequencing_center",
        "sex"::TEXT AS "sex"
        ,
        NULL AS "solve_state",
        "subject_id"::TEXT AS "subject_id"
        ,
        NULL AS "twin_id"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/subject_ANVIL_CMG_UWASH_HMB_IRB_20250219_ANV5_202502201921_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'affected_status': 'VARCHAR'
        ,'ancestry': 'VARCHAR'
        ,'dbgap_submission': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'multiple_datasets': 'VARCHAR'
        ,'phenotype_description': 'VARCHAR'
        ,'proband_relationship': 'VARCHAR'
        ,'project_id': 'VARCHAR'
        ,'sex': 'VARCHAR'
        ,'hpo_present': 'VARCHAR'
        ,'maternal_id': 'VARCHAR'
        ,'paternal_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "affected_status"::TEXT AS "affected_status"
        ,
        NULL AS "age_at_last_observation",
        "ancestry"::TEXT AS "ancestry"
        ,
        "ancestry_detail"::TEXT AS "ancestry_detail"
        ,
        "congenital_status"::TEXT AS "congenital_status"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "dbgap_study_id",
        NULL AS "dbgap_subject_id",
        "dbgap_submission"::TEXT AS "dbgap_submission"
        ,
        NULL AS "disease_description",
        "disease_id"::TEXT AS "disease_id"
        ,
        "family_id"::TEXT AS "family_id"
        ,
        "hpo_present"::TEXT AS "hpo_present"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "maternal_id"::TEXT AS "maternal_id"
        ,
        "multiple_datasets"::TEXT AS "multiple_datasets"
        ,
        "paternal_id"::TEXT AS "paternal_id"
        ,
        "phenotype_description"::TEXT AS "phenotype_description"
        ,
        NULL AS "pmid_id",
        NULL AS "prior_testing",
        "proband_relationship"::TEXT AS "proband_relationship"
        ,
        "project_id"::TEXT AS "project_id"
        ,
        NULL AS "sample_id",
        NULL AS "sequencing_center",
        "sex"::TEXT AS "sex"
        ,
        NULL AS "solve_state",
        "subject_id"::TEXT AS "subject_id"
        ,
        NULL AS "twin_id"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/subject_ANVIL_CMG_UWASH_HMB_20250219_ANV5_202502201916_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'affected_status': 'VARCHAR'
        ,'ancestry': 'VARCHAR'
        ,'dbgap_submission': 'VARCHAR'
        ,'disease_id': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'hpo_present': 'VARCHAR'
        ,'multiple_datasets': 'VARCHAR'
        ,'phenotype_description': 'VARCHAR'
        ,'proband_relationship': 'VARCHAR'
        ,'project_id': 'VARCHAR'
        ,'sex': 'VARCHAR'
        ,'paternal_id': 'VARCHAR'
        ,'maternal_id': 'VARCHAR'
        ,'ancestry_detail': 'VARCHAR'
        ,'congenital_status': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "affected_status"::TEXT AS "affected_status"
        ,
        NULL AS "age_at_last_observation",
        "ancestry"::TEXT AS "ancestry"
        ,
        NULL AS "ancestry_detail",
        NULL AS "congenital_status",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "dbgap_study_id",
        NULL AS "dbgap_subject_id",
        "dbgap_submission"::TEXT AS "dbgap_submission"
        ,
        NULL AS "disease_description",
        "disease_id"::TEXT AS "disease_id"
        ,
        "family_id"::TEXT AS "family_id"
        ,
        "hpo_present"::TEXT AS "hpo_present"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "maternal_id"::TEXT AS "maternal_id"
        ,
        "multiple_datasets"::TEXT AS "multiple_datasets"
        ,
        "paternal_id"::TEXT AS "paternal_id"
        ,
        "phenotype_description"::TEXT AS "phenotype_description"
        ,
        NULL AS "pmid_id",
        NULL AS "prior_testing",
        "proband_relationship"::TEXT AS "proband_relationship"
        ,
        "project_id"::TEXT AS "project_id"
        ,
        NULL AS "sample_id",
        NULL AS "sequencing_center",
        "sex"::TEXT AS "sex"
        ,
        NULL AS "solve_state",
        "subject_id"::TEXT AS "subject_id"
        ,
        NULL AS "twin_id"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/subject_ANVIL_CMG_UWASH_DS_NBIA_20250206_ANV5_202502201903_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'affected_status': 'VARCHAR'
        ,'ancestry': 'VARCHAR'
        ,'dbgap_submission': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'hpo_present': 'VARCHAR'
        ,'maternal_id': 'VARCHAR'
        ,'multiple_datasets': 'VARCHAR'
        ,'paternal_id': 'VARCHAR'
        ,'phenotype_description': 'VARCHAR'
        ,'proband_relationship': 'VARCHAR'
        ,'project_id': 'VARCHAR'
        ,'sex': 'VARCHAR'
        ,'disease_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "affected_status"::TEXT AS "affected_status"
        ,
        NULL AS "age_at_last_observation",
        NULL AS "ancestry",
        NULL AS "ancestry_detail",
        NULL AS "congenital_status",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "dbgap_study_id",
        NULL AS "dbgap_subject_id",
        "dbgap_submission"::TEXT AS "dbgap_submission"
        ,
        NULL AS "disease_description",
        NULL AS "disease_id",
        "family_id"::TEXT AS "family_id"
        ,
        "hpo_present"::TEXT AS "hpo_present"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "maternal_id"::TEXT AS "maternal_id"
        ,
        "multiple_datasets"::TEXT AS "multiple_datasets"
        ,
        "paternal_id"::TEXT AS "paternal_id"
        ,
        "phenotype_description"::TEXT AS "phenotype_description"
        ,
        NULL AS "pmid_id",
        NULL AS "prior_testing",
        "proband_relationship"::TEXT AS "proband_relationship"
        ,
        "project_id"::TEXT AS "project_id"
        ,
        NULL AS "sample_id",
        NULL AS "sequencing_center",
        "sex"::TEXT AS "sex"
        ,
        NULL AS "solve_state",
        "subject_id"::TEXT AS "subject_id"
        ,
        NULL AS "twin_id"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/subject_ANVIL_CMG_UWASH_DS_HFA_20250206_ANV5_202502201859_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'affected_status': 'VARCHAR'
        ,'dbgap_submission': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'multiple_datasets': 'VARCHAR'
        ,'phenotype_description': 'VARCHAR'
        ,'proband_relationship': 'VARCHAR'
        ,'project_id': 'VARCHAR'
        ,'sex': 'VARCHAR'
        ,'hpo_present': 'VARCHAR'
        ,'maternal_id': 'VARCHAR'
        ,'paternal_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "affected_status"::TEXT AS "affected_status"
        ,
        NULL AS "age_at_last_observation",
        "ancestry"::TEXT AS "ancestry"
        ,
        NULL AS "ancestry_detail",
        NULL AS "congenital_status",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "dbgap_study_id",
        NULL AS "dbgap_subject_id",
        "dbgap_submission"::TEXT AS "dbgap_submission"
        ,
        NULL AS "disease_description",
        "disease_id"::TEXT AS "disease_id"
        ,
        "family_id"::TEXT AS "family_id"
        ,
        "hpo_present"::TEXT AS "hpo_present"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "maternal_id"::TEXT AS "maternal_id"
        ,
        "multiple_datasets"::TEXT AS "multiple_datasets"
        ,
        "paternal_id"::TEXT AS "paternal_id"
        ,
        "phenotype_description"::TEXT AS "phenotype_description"
        ,
        NULL AS "pmid_id",
        NULL AS "prior_testing",
        "proband_relationship"::TEXT AS "proband_relationship"
        ,
        "project_id"::TEXT AS "project_id"
        ,
        NULL AS "sample_id",
        NULL AS "sequencing_center",
        "sex"::TEXT AS "sex"
        ,
        "solve_state"::TEXT AS "solve_state"
        ,
        "subject_id"::TEXT AS "subject_id"
        ,
        NULL AS "twin_id"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/subject_ANVIL_CMG_UWash_DS_EP_20250219_ANV5_202502201854_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'affected_status': 'VARCHAR'
        ,'ancestry': 'VARCHAR'
        ,'dbgap_submission': 'VARCHAR'
        ,'disease_id': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'multiple_datasets': 'VARCHAR'
        ,'phenotype_description': 'VARCHAR'
        ,'proband_relationship': 'VARCHAR'
        ,'project_id': 'VARCHAR'
        ,'sex': 'VARCHAR'
        ,'hpo_present': 'VARCHAR'
        ,'maternal_id': 'VARCHAR'
        ,'paternal_id': 'VARCHAR'
        ,'solve_state': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "affected_status"::TEXT AS "affected_status"
        ,
        NULL AS "age_at_last_observation",
        "ancestry"::TEXT AS "ancestry"
        ,
        "ancestry_detail"::TEXT AS "ancestry_detail"
        ,
        NULL AS "congenital_status",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "dbgap_study_id",
        NULL AS "dbgap_subject_id",
        "dbgap_submission"::TEXT AS "dbgap_submission"
        ,
        NULL AS "disease_description",
        "disease_id"::TEXT AS "disease_id"
        ,
        "family_id"::TEXT AS "family_id"
        ,
        "hpo_present"::TEXT AS "hpo_present"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "maternal_id"::TEXT AS "maternal_id"
        ,
        "multiple_datasets"::TEXT AS "multiple_datasets"
        ,
        "paternal_id"::TEXT AS "paternal_id"
        ,
        "phenotype_description"::TEXT AS "phenotype_description"
        ,
        NULL AS "pmid_id",
        NULL AS "prior_testing",
        "proband_relationship"::TEXT AS "proband_relationship"
        ,
        "project_id"::TEXT AS "project_id"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        "sequencing_center"::TEXT AS "sequencing_center"
        ,
        "sex"::TEXT AS "sex"
        ,
        NULL AS "solve_state",
        "subject_id"::TEXT AS "subject_id"
        ,
        NULL AS "twin_id"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/subject_ANVIL_CMG_UWASH_DS_BDIS_20250206_ANV5_202502201850_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'affected_status': 'VARCHAR'
        ,'ancestry': 'VARCHAR'
        ,'ancestry_detail': 'VARCHAR'
        ,'dbgap_submission': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'hpo_present': 'VARCHAR'
        ,'maternal_id': 'VARCHAR'
        ,'multiple_datasets': 'VARCHAR'
        ,'paternal_id': 'VARCHAR'
        ,'phenotype_description': 'VARCHAR'
        ,'proband_relationship': 'VARCHAR'
        ,'project_id': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sequencing_center': 'VARCHAR'
        ,'sex': 'VARCHAR'
        ,'disease_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "affected_status"::TEXT AS "affected_status"
        ,
        NULL AS "age_at_last_observation",
        "ancestry"::TEXT AS "ancestry"
        ,
        NULL AS "ancestry_detail",
        NULL AS "congenital_status",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "dbgap_study_id",
        NULL AS "dbgap_subject_id",
        "dbgap_submission"::TEXT AS "dbgap_submission"
        ,
        NULL AS "disease_description",
        NULL AS "disease_id",
        "family_id"::TEXT AS "family_id"
        ,
        "hpo_present"::TEXT AS "hpo_present"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "maternal_id"::TEXT AS "maternal_id"
        ,
        "multiple_datasets"::TEXT AS "multiple_datasets"
        ,
        "paternal_id"::TEXT AS "paternal_id"
        ,
        "phenotype_description"::TEXT AS "phenotype_description"
        ,
        NULL AS "pmid_id",
        NULL AS "prior_testing",
        "proband_relationship"::TEXT AS "proband_relationship"
        ,
        "project_id"::TEXT AS "project_id"
        ,
        NULL AS "sample_id",
        NULL AS "sequencing_center",
        "sex"::TEXT AS "sex"
        ,
        NULL AS "solve_state",
        "subject_id"::TEXT AS "subject_id"
        ,
        NULL AS "twin_id"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/subject_ANVIL_CMG_UWASH_DS_BAV_IRB_PUB_RD_20250206_ANV5_202502201846_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'affected_status': 'VARCHAR'
        ,'ancestry': 'VARCHAR'
        ,'dbgap_submission': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'hpo_present': 'VARCHAR'
        ,'multiple_datasets': 'VARCHAR'
        ,'paternal_id': 'VARCHAR'
        ,'phenotype_description': 'VARCHAR'
        ,'proband_relationship': 'VARCHAR'
        ,'project_id': 'VARCHAR'
        ,'sex': 'VARCHAR'
        ,'maternal_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source
