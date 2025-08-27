{{ config(materialized='table') }}
{%- set relation = source('cmg_yale', 'subject') -%}
{%- set constant_columns = get_columns(relation=relation, exclude=[]) -%}

with source as (
    select 
      ROW_NUMBER() OVER () AS ftd_index,
      "subject_id"::text as "subject_id",
       "affected_status"::text as "affected_status",
       "ancestry"::text as "ancestry",
       "ancestry_detail"::text as "ancestry_detail",
       "dbgap_submission"::text as "dbgap_submission",
       "disease_description"::text as "disease_description",
       "family_id"::text as "family_id",
       "hpo_present"::text as "hpo_present",
       "maternal_id"::text as "maternal_id",
       "multiple_datasets"::text as "multiple_datasets",
       "paternal_id"::text as "paternal_id",
       "proband_relationship"::text as "proband_relationship",
       "project_id"::text as "project_id",
       "dbgap_study_id"::text as "dbgap_study_id",
       "sex"::text as "sex",
       "ingest_provenance"::text as "ingest_provenance",
       "disease_id"::text as "disease_id",
       "sample_id"::text as "sample_id"
    from {{ source('cmg_yale','subject') }}
)

,clean_codes as (
    select
      {{ constant_columns | join(', ') }}, ftd_index,
      {{ clean_codes('hpo_present',['HP:','HPO:'], ['Ê', '"', "''"]) }} as "clean_hpo_present",
        case
        when disease_id = 'PS173900|'
        then 'PS173900' 
        when disease_id like '%OMIM%'
        then {{ clean_codes('disease_id',['OMIM:'], ['Ê', '"', "''"]) }} 
        else disease_id 
       end as "clean_disease_id"
    from source as s
)
,unaffected as (
    select
      {{ constant_columns | join(', ') }},
      ftd_index,
      NULL as presence,
      NULL as c_or_d,
      NULL as c_code,
      NULL as d_code
    from clean_codes
    where hpo_present is null 
      and disease_id is null
)
,unpivot_df as (
    select distinct *
    from (
        select
          {{ constant_columns | join(', ') }},
          ftd_index,
          'Affected' as presence,
          'condition' as c_or_d,
          unnest(str_split(clean_hpo_present, '|')) as c_code,
          NULL as d_code
        from clean_codes
        where clean_hpo_present is not null
         
        union all

        select
          {{ constant_columns | join(', ') }},
          ftd_index,
          affected_status as presence,
          'disease' as c_or_d,
          NULL as c_code,
          unnest(str_split(clean_disease_id, '|')) as d_code,
        from  clean_codes
        where clean_disease_id is not null
)        
    where c_code is not null
      or d_code is not null
)

select 
   distinct
   ftd_index,
   subject_id,
   affected_status,
   ancestry,
   ancestry_detail,
   dbgap_submission,
   disease_description,
   family_id,
   hpo_present,
   maternal_id,
   multiple_datasets,
   paternal_id,
   proband_relationship,
   project_id,
   dbgap_study_id,
   sex,
   ingest_provenance,
   REPLACE(REPLACE(REPLACE(UPPER(ingest_provenance), 'SUBJECT_ANVIL_CMG_YALE_', ''), 'PARTICIPANT_ANVIL_CMG_YALE_', ''),'.TSV','') AS "consent_id",
   disease_id,
   sample_id,
   presence,
   c_or_d,
   c_code as "condition_code",
   d_code as "disease_code",
   coalesce(c_code,d_code,null) as "condition_or_disease_code"
from (select * from unpivot_df
      union all
      select * from unaffected)
order by ftd_index