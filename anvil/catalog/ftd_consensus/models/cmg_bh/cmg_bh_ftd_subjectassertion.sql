{{ config(materialized='table', schema='cmg_bh_data') }}
{%- set relation = ref('cmg_bh_stg_subject') -%}
{%- set constant_columns = ['subject_id','submission_batch','affected_status',
 'ancestry','dbgap_study_id','dbgap_submission','family_id','family_relationship',
 'hpo_present','maternal_id','multiple_datasets','paternal_id','phenotype_description',
 'phenotype_group','project_investigator','sex','solve_state','disease_id','ancestry_detail','hpo_absent',
 'twin_id','ingest_provenance'] -%}
with
lookup as (
    select
      "searched_code" as code,
      "display"
    from {{ ref('cmg_bh_annotations') }}
)
,clean_codes as (
    select
      {{ constant_columns | join(', ') }},
      {{ clean_codes('hpo_absent', ['HP:','HPO:'], ['Ê', '"', "''"]) }} as "clean_hpo_absent",
      {{ clean_codes('hpo_present',['HP:','HPO:'], ['Ê', '"', "''"]) }} as "clean_hpo_present"
    from {{ ref('cmg_bh_stg_subject') }} as s
)
,unpivot_df as (
    select distinct *
    from (
        select
          {{ constant_columns | join(', ') }},
          'hpo_present' as hpo_presence,
           unnest(str_split(cc.clean_hpo_present, '|')) as code
        from clean_codes as cc

        union all

        select
          {{ constant_columns | join(', ') }},
          'hpo_absent' as hpo_presence,
          unnest(str_split(cc.clean_hpo_absent, '|')) as code
        from  clean_codes as cc
          )
    where code is not null
)
,source as (
    select 
      up.code as "code",
      lookup.display as "display",
      up.hpo_presence,
        case
        when up.hpo_presence = 'hpo_present' then 'Yes'
        when up.hpo_presence = 'hpo_absent' then 'No'
        else null
      end::text as "value_code",
        case
        when up.hpo_presence = 'hpo_present' then 'Present'
        when up.hpo_presence = 'hpo_absent' then 'Absent'
        else null
      end::text as "value_display",
      {{ generate_global_id(prefix='ap',descriptor=['up.subject_id'], study_id='cmg_bh') }}::text as "has_access_policy",
      {{ generate_global_id(prefix='sa',descriptor=['up.subject_id'], study_id='cmg_bh') }}::text as "id",
      {{ generate_global_id(prefix='sb',descriptor=['up.subject_id'], study_id='cmg_bh') }}::text as "subject_id"
    from unpivot_df as up
    join lookup
    on up.code = lookup.code
    )

select 
  'phenotypic_feature'::text as "assertion_type",
   NULL::text as "age_at_assertion",
   NULL::text as "age_at_event",
   NULL::text as "age_at_resolution",
   source.code,
   source.display,
   source.value_code,
   source.value_display,
   NULL::text as "value_number",
   NULL::text as "value_units",
   NULL::text as "value_units_display",
   source.has_access_policy,
   source.id,
   source.subject_id
from source
    