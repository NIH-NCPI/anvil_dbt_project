{{ config(materialized='table', schema='cmg_yale_data') }}

select 
GEN_UNKNOWN.code::text as "code",
  GEN_UNKNOWN.display::text as "display",
  GEN_UNKNOWN.value_code::text as "value_code",
  GEN_UNKNOWN.value_display::text as "value_display",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "id"
from {{ ref('cmg_yale_stg_sample') }} as sample
join {{ ref('cmg_yale_stg_subject') }} as subject
on sample.subject_id = subject.subject_id  join {{ ref('cmg_yale_stg_anvil_dataset') }} as anvil_dataset
on   join {{ ref('cmg_yale_stg_sequencing') }} as sequencing
on   join {{ ref('cmg_yale_stg_family') }} as family
on  

