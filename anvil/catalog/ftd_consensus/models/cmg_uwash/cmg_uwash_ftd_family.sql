{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
GEN_UNKNOWN.family_type::text as "family_type",
  GEN_UNKNOWN.family_description::text as "family_description",
  family.consanguinity::text as "consanguinity",
  GEN_UNKNOWN.family_study_focus::text as "family_study_focus",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_uwash') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_uwash') }}::text as "id"
from {{ ref('cmg_uwash_stg_sample') }} as sample
join {{ ref('cmg_uwash_stg_subject') }} as subject
on sample.subject_id = subject.subject_id  join {{ ref('cmg_uwash_stg_anvil_dataset') }} as anvil_dataset
on   join {{ ref('cmg_uwash_stg_sequencing') }} as sequencing
on   join {{ ref('cmg_uwash_stg_family') }} as family
on   join {{ ref('cmg_uwash_stg_file_inventory') }} as file_inventory
on  

