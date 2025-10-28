{{ config(materialized='table', schema='cmg_uwash_data') }}

select
  distinct
  registered_identifier::text as "parent_study_id",
  concat('University of Washington Center for Mendelian Genomics (UW-CMG) ',consent_group) as "study_title",
  {{ generate_global_id(prefix='st',descriptor=['dataset_id'], study_id='phs000693') }}::text as "id",
from {{ ref('cmg_uwash_stg_anvil_dataset') }} as ad