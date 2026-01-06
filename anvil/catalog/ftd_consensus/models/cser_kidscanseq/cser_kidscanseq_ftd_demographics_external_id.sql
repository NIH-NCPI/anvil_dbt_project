{{ config(materialized='table', schema='cser_kidscanseq_data') }}

select 
  {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_kidscanseq') }}::text as "demographics_id",
  GEN_UNKNOWN.external_id::text as "external_id"
from {{ ref('cser_kidscanseq_stg_anvil_dataset') }} as anvil_dataset
join {{ ref('cser_kidscanseq_stg_file_inventory') }} as file_inventory
on   join {{ ref('cser_kidscanseq_stg_sample') }} as sample
on subject.subject_id = sample.subject_id  join {{ ref('cser_kidscanseq_stg_sequencing') }} as sequencing
on   join {{ ref('cser_kidscanseq_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 

