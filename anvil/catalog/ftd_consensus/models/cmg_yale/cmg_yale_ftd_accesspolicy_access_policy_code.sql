{{ config(materialized='table', schema='cmg_yale_data') }}

select 
  {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "accesspolicy_id",
  GEN_UNKNOWN.access_policy_code::text as "access_policy_code"
from {{ ref('cmg_yale_stg_sample') }} as sample
join {{ ref('cmg_yale_stg_subject') }} as subject
on sample.subject_id = subject.subject_id  join {{ ref('cmg_yale_stg_anvil_dataset') }} as anvil_dataset
on   join {{ ref('cmg_yale_stg_sequencing') }} as sequencing
on   join {{ ref('cmg_yale_stg_family') }} as family
on  

