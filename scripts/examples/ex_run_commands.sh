dbt clean
dbt deps  || { echo "Error: dbt deps failed. Exiting..."; exit 1; }
dbt seed
# 

dbt run --select  GWAS_stg_subjectconsent
dbt run --select  GWAS_stg_bmi
dbt run --select  GWAS_stg_phecode
# dbt run --select  PGRNseq_stg_demographics
# dbt run --select  PGRNseq_stg_sampleattributions
# dbt run --select  PGRNseq_stg_samplesubjectmapping
# dbt run --select  PGRNseq_stg_ecg
# dbt run --select  PGRNseq_stg_labs

# dbt build --select  PGRNseq_ftd_accesspolicy
# dbt build --select  PGRNseq_ftd_accesspolicy_access_policy_code
# dbt build --select  PGRNseq_ftd_demographics
# dbt run --select  PGRNseq_ftd_demographics_external_id
# dbt build --select  PGRNseq_ftd_demographics_race
# dbt build --select  PGRNseq_ftd_sample
# dbt run --select  PGRNseq_ftd_sample_external_id
# dbt build --select  PGRNseq_ftd_subject
# dbt run --select  PGRNseq_ftd_subject_external_id
# dbt build --select  PGRNseq_ftd_subjectassertion
dbt build --select GWAS_ftd_subjectassertion

# dbt run --select tgt_participant --vars '{"source_table": "gregor_synthetic_ftd_participant", "target_schema": "main"}'
# dbt run --select tgt_phenotype --vars '{"source_table": "gregor_synthetic_ftd_phenotype", "target_schema": "main"}'

# Run a single target model and its dependencies.
# dbt run --select tgt_participant --vars '{"source_table": "gregor_synthetic_ftd_participant", "target_schema": "gregor_synthetic_tgt_data"}'
# dbt run --select tgt_condition --vars '{"source_table": "gregor_synthetic_ftd_condition", "target_schema": "gregor_synthetic_tgt_data"}'
# dbt run --select +tgt_event --vars '{"source_table": "gregor_synthetic_ftd_event", "target_schema": "gregor_synthetic_tgt_data"}'
# dbt run --select +tgt_measurement --vars '{"source_table": "gregor_synthetic_ftd_measurement", "target_schema": "gregor_synthetic_tgt_data"}'
# dbt run --select tgt_study --vars '{"source_table": "gregor_synthetic_ftd_study", "target_schema": "gregor_synthetic_tgt_data"}'
