{{ config(materialized='table', schema='cmg_bh_data') }}

with
lookup as (
    select -- annotations from src
      "searched_code" as join_code,
      "searched_code" as code,
      "display" as display
    from {{ ref('cmg_bh_annotations_code') }}
    
    union all
    
    select -- annotations from MD
      "local code" as join_code,
      "code" as code,
      "display" as display
    from {{ ref('subject_mappings') }}
)

select 
  'phenotypic_feature'::text as "assertion_type",
  NULL::text as "age_at_assertion",
  NULL::text as "age_at_event",
  NULL::text as "age_at_resolution",
  lookup.code as "code",
  lookup.display as "display",
    case
    when presence = 'Affected' then 'affected'
    when presence = 'Unaffected' then 'unaffected'
    when presence = 'Unknown' then 'unknown'
    else null
  end::text as "value_code",
    case
    when presence = 'Affected' then 'Affected'
    when presence = 'Unaffected' then 'Unaffected'
    when presence = 'Unknown' then 'Unknown'
    else null
  end::text as "value_display",  
  NULL::text as "value_number",
  NULL::text as "value_units",
  NULL::text as "value_units_display",
  {{ generate_global_id(prefix='ap',descriptor=['subject_id'], study_id='cmg_bh') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='sa',descriptor=['subject_id','code'], study_id='cmg_bh') }}::text as "id",
  {{ generate_global_id(prefix='sb',descriptor=['subject_id'], study_id='cmg_bh') }}::text as "subject_id"
from {{ ref('cmg_bh_stg_subject') }}
join lookup
on code = lookup.join_code