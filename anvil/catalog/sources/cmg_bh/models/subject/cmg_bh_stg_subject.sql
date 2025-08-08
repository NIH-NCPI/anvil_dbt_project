{{ config(materialized='table') }}
{%- set relation = source('cmg_bh', 'subject') -%}
{%- set constant_columns = get_columns(relation=relation, exclude=[]) -%}
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
    from (select *, ROW_NUMBER() OVER () AS ftd_index from {{ source('cmg_bh','subject') }}) as s
)
,clean_codes as (
    select
      {{ constant_columns | join(', ') }}, ftd_index,
      {{ clean_codes('hpo_absent', ['HP:','HPO:'], ['Ê', '"', "''"]) }} as "clean_hpo_absent",
      {{ clean_codes('hpo_present',['HP:','HPO:'], ['Ê', '"', "''"]) }} as "clean_hpo_present"
    from source as s
)
,unpivot_df as (
    select distinct *
    from (
        select
          {{ constant_columns | join(', ') }},
          ftd_index,
          'Affected' as presence,
           unnest(str_split(cc.clean_hpo_present, '|')) as code
        from clean_codes as cc

        union all

        select
          {{ constant_columns | join(', ') }},
          ftd_index,
          'Unaffected' as presence,
          unnest(str_split(cc.clean_hpo_absent, '|')) as code
        from  clean_codes as cc
         
        union all

        select
          {{ constant_columns | join(', ') }},
          ftd_index,
          cc.affected_status as presence,
          cc.disease_id as code
        from  clean_codes as cc
        
        union all

        select
          {{ constant_columns | join(', ') }},
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
    