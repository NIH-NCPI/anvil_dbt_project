{{ config(materialized='table', schema='cmg_bh_data') }}
{%- set relation = ref('cmg_bh_stg_subject') -%}
{% set constant_columns = get_columns(relation=relation, exclude=[]) %}
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
,source as (
    select 
      lookup.code as "code",
      lookup.display as "display",
        case
        when up.presence = 'Affected' then 'affected'
        when up.presence = 'Unaffected' then 'unaffected'
        when up.presence = 'Unknown' then 'unknown'
        else null
      end::text as "value_code",
        case
        when up.presence = 'Affected' then 'Affected'
        when up.presence = 'Unaffected' then 'Unaffected'
        when up.presence = 'Unknown' then 'Unknown'
        else null
      end::text as "value_display",
      {{ generate_global_id(prefix='ap',descriptor=['up.subject_id'], study_id='cmg_bh') }}::text as "has_access_policy",
      {{ generate_global_id(prefix='sa',descriptor=['up.subject_id'], study_id='cmg_bh') }}::text as "id",
      {{ generate_global_id(prefix='sb',descriptor=['up.subject_id'], study_id='cmg_bh') }}::text as "subject_id"
    from unpivot_df as up
    join lookup
    on up.code = lookup.join_code
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