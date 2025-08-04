{{ config(materialized='table', schema='cmg_bh_data') }}
{%- set relation = ref('cmg_bh_stg_subject') -%}
{%- set constant_columns = get_columns(relation=relation, exclude=[]) -%}
with
lookup as (
    select
      "searched_code" as join_code,
      "searched_code" as code,
      "display" as display
    from {{ ref('cmg_bh_annotations_code') }}
    
    union all
    
    select
      "local code" as join_code,
      "code" as code,
      "display" as display
    from {{ ref('subject_mappings') }}
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
          'Affected' as presence,
           unnest(str_split(cc.clean_hpo_present, '|')) as code
        from clean_codes as cc

        union all

        select
          {{ constant_columns | join(', ') }},
          'Unaffected' as presence,
          unnest(str_split(cc.clean_hpo_absent, '|')) as code
        from  clean_codes as cc
         
        union all

        select
          {{ constant_columns | join(', ') }},
          cc.affected_status as presence,
          cc.disease_id as code
        from  clean_codes as cc
        
        union all

        select
          {{ constant_columns | join(', ') }},
          cc.affected_status as presence,
          cc.phenotype_description as code
        from  clean_codes as cc
          )
    where code is not null
)

select 
   {{ generate_global_id(prefix='sa',descriptor=['code'], study_id='cmg_bh') }}::text as "id",
   NULL::text as "external_id"
from unpivot_df    