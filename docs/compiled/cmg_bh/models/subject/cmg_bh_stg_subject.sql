with source as (
    select 
       ftd_index,
       "subject_id"::text as "subject_id",
       "submission_batch"::text as "submission_batch",
       "affected_status"::text as "affected_status",
       "ancestry"::text as "ancestry",
       "dbgap_study_id"::text as "dbgap_study_id",
       "dbgap_submission"::text as "dbgap_submission",
       "family_id"::text as "family_id",
       "family_relationship"::text as "family_relationship",
       "hpo_present"::text as "hpo_present",
       "maternal_id"::text as "maternal_id",
       "multiple_datasets"::text as "multiple_datasets",
       "paternal_id"::text as "paternal_id",
       "phenotype_description"::text as "phenotype_description",
       "phenotype_group"::text as "phenotype_group",
       "project_investigator"::text as "project_investigator",
       "sex"::text as "sex",
       "solve_state"::text as "solve_state",
       "disease_id"::text as "disease_id",
       "ancestry_detail"::text as "ancestry_detail",
       "hpo_absent"::text as "hpo_absent",
       "twin_id"::text as "twin_id",
       "ingest_provenance"::text as "ingest_provenance"
    from (select *, ROW_NUMBER() OVER () AS ftd_index from "dbt"."main"."subject") as s
)
,clean_codes as (
    select
      subject_id, submission_batch, affected_status, ancestry, dbgap_study_id, dbgap_submission, family_id, family_relationship, hpo_present, maternal_id, multiple_datasets, paternal_id, phenotype_description, phenotype_group, project_investigator, sex, solve_state, disease_id, ancestry_detail, hpo_absent, twin_id, ingest_provenance, ftd_index,
      (
    select string_agg(distinct trim(code), '|' order by trim(code))
    from (
        select unnest(string_to_array(trim(both '|' 
        from translate(replace(replace(hpo_absent, 'HP:', '|HP:'), 'HPO:', '|HPO:'), 'Ê"''', '')),'|')) as code
        ) as split_codes
    where code is not null and length(trim(code)) > 0
) as "clean_hpo_absent",
      (
    select string_agg(distinct trim(code), '|' order by trim(code))
    from (
        select unnest(string_to_array(trim(both '|' 
        from translate(replace(replace(hpo_present, 'HP:', '|HP:'), 'HPO:', '|HPO:'), 'Ê"''', '')),'|')) as code
        ) as split_codes
    where code is not null and length(trim(code)) > 0
) as "clean_hpo_present"
    from source as s
)
,unpivot_df as (
    select distinct *
    from (
        select
          subject_id, submission_batch, affected_status, ancestry, dbgap_study_id, dbgap_submission, family_id, family_relationship, hpo_present, maternal_id, multiple_datasets, paternal_id, phenotype_description, phenotype_group, project_investigator, sex, solve_state, disease_id, ancestry_detail, hpo_absent, twin_id, ingest_provenance,
          ftd_index,
          'Affected' as presence,
           unnest(str_split(cc.clean_hpo_present, '|')) as code
        from clean_codes as cc

        union all

        select
          subject_id, submission_batch, affected_status, ancestry, dbgap_study_id, dbgap_submission, family_id, family_relationship, hpo_present, maternal_id, multiple_datasets, paternal_id, phenotype_description, phenotype_group, project_investigator, sex, solve_state, disease_id, ancestry_detail, hpo_absent, twin_id, ingest_provenance,
          ftd_index,
          'Unaffected' as presence,
          unnest(str_split(cc.clean_hpo_absent, '|')) as code
        from  clean_codes as cc
         
        union all

        select
          subject_id, submission_batch, affected_status, ancestry, dbgap_study_id, dbgap_submission, family_id, family_relationship, hpo_present, maternal_id, multiple_datasets, paternal_id, phenotype_description, phenotype_group, project_investigator, sex, solve_state, disease_id, ancestry_detail, hpo_absent, twin_id, ingest_provenance,
          ftd_index,
          cc.affected_status as presence,
          cc.disease_id as code
        from  clean_codes as cc
        
        union all

        select
          subject_id, submission_batch, affected_status, ancestry, dbgap_study_id, dbgap_submission, family_id, family_relationship, hpo_present, maternal_id, multiple_datasets, paternal_id, phenotype_description, phenotype_group, project_investigator, sex, solve_state, disease_id, ancestry_detail, hpo_absent, twin_id, ingest_provenance,
          ftd_index,
          cc.affected_status as presence,
          cc.phenotype_description as code
        from  clean_codes as cc
          )
    where code is not null
)

select 
   ftd_index,
   subject_id,
   submission_batch,
   affected_status,
   ancestry,
   dbgap_study_id,
   dbgap_submission,
   family_id,
   family_relationship,
   hpo_present,
   maternal_id,
   multiple_datasets,
   paternal_id,
   phenotype_description,
   phenotype_group,
   project_investigator,
   sex,
   solve_state,
   disease_id,
   ancestry_detail,
   hpo_absent,
   twin_id,
   ingest_provenance,
   presence,
   code as "condition_or_disease_code"
from unpivot_df