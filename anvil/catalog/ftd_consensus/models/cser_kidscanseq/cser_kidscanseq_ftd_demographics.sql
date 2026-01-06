{{ config(materialized='table', schema='cser_kidscanseq_data') }}

select 
GEN_UNKNOWN.date_of_birth::integer as "date_of_birth",
  GEN_UNKNOWN.date_of_birth_type::text as "date_of_birth_type",
  subject.sex::text as "sex",
  GEN_UNKNOWN.sex_display::text as "sex_display",
  GEN_UNKNOWN.race_display::text as "race_display",
  GEN_UNKNOWN.ethnicity::text as "ethnicity",
  GEN_UNKNOWN.ethnicity_display::text as "ethnicity_display",
  GEN_UNKNOWN.age_at_last_vital_status::integer as "age_at_last_vital_status",
  GEN_UNKNOWN.vital_status::text as "vital_status",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_kidscanseq') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_kidscanseq') }}::text as "id"
from {{ ref('cser_kidscanseq_stg_anvil_dataset') }} as anvil_dataset
join {{ ref('cser_kidscanseq_stg_file_inventory') }} as file_inventory
on   join {{ ref('cser_kidscanseq_stg_sample') }} as sample
on subject.subject_id = sample.subject_id  join {{ ref('cser_kidscanseq_stg_sequencing') }} as sequencing
on   join {{ ref('cser_kidscanseq_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 

