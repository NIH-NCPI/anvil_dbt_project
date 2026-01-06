{{ config(materialized='table', schema='cser_kidscanseq_data') }}

select 
  {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_kidscanseq') }}::text as "parent_sample",
  GEN_UNKNOWN.sample_type::text as "sample_type",
  GEN_UNKNOWN.availablity_status::text as "availablity_status",
  GEN_UNKNOWN.quantity_number::text as "quantity_number",
  GEN_UNKNOWN.quantity_units::text as "quantity_units",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_kidscanseq') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_kidscanseq') }}::text as "id",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_kidscanseq') }}::text as "subject_id",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_kidscanseq') }}::text as "biospecimen_collection_id"
from {{ ref('cser_kidscanseq_stg_anvil_dataset') }} as anvil_dataset
join {{ ref('cser_kidscanseq_stg_file_inventory') }} as file_inventory
on   join {{ ref('cser_kidscanseq_stg_sample') }} as sample
on subject.subject_id = sample.subject_id  join {{ ref('cser_kidscanseq_stg_sequencing') }} as sequencing
on   join {{ ref('cser_kidscanseq_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 

