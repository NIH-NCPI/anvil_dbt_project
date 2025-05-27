dbt clean
dbt deps  || { echo "Error: dbt deps failed. Exiting..."; exit 1; }
# dbt seed
# 

# gregor/duckdb examples
dbt run --select  gregor_synthetic_stg_participant
dbt run --select  gregor_synthetic_stg_phenotype
dbt run --select  gregor_synthetic_ftd_subject
dbt run --select  gregor_synthetic_ftd_subject_external_id
dbt run --select  gregor_synthetic_ftd_subjectassertion
dbt run --select  gregor_synthetic_ftd_subjectassertion_external_id
dbt run --select  gregor_synthetic_ftd_demographics
dbt run --select  gregor_synthetic_ftd_demographics_external_id
dbt run --select  gregor_synthetic_ftd_demographics_race
dbt run --select  gregor_synthetic_ftd_familymember
dbt run --select  gregor_synthetic_ftd_familymember_external_id
dbt run --select  gregor_synthetic_ftd_familyrelationship
dbt run --select  gregor_synthetic_ftd_familyrelationship_external_id
dbt run --select  gregor_synthetic_ftd_accesspolicy_access_policy_code
# dbt run --select  gregor_synthetic_ftd_participant
# dbt run --select  gregor_synthetic_ftd_phenotype
# dbt run --select tgt_participant --vars '{"source_table": "gregor_synthetic_ftd_participant", "target_schema": "main"}'
# dbt run --select tgt_phenotype --vars '{"source_table": "gregor_synthetic_ftd_phenotype", "target_schema": "main"}'

# Run a single target model and its dependencies.
# dbt run --select tgt_participant --vars '{"source_table": "gregor_synthetic_ftd_participant", "target_schema": "gregor_synthetic_tgt_data"}'
# dbt run --select tgt_condition --vars '{"source_table": "gregor_synthetic_ftd_condition", "target_schema": "gregor_synthetic_tgt_data"}'
# dbt run --select +tgt_event --vars '{"source_table": "gregor_synthetic_ftd_event", "target_schema": "gregor_synthetic_tgt_data"}'
# dbt run --select +tgt_measurement --vars '{"source_table": "gregor_synthetic_ftd_measurement", "target_schema": "gregor_synthetic_tgt_data"}'
# dbt run --select tgt_study --vars '{"source_table": "gregor_synthetic_ftd_study", "target_schema": "gregor_synthetic_tgt_data"}'
