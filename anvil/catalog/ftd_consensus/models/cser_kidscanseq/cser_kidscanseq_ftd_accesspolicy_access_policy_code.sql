{{ config(materialized='table', schema='cser_kidscanseq_data') }}

select 
  {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='cser_kidscanseq') }}::text as "accesspolicy_id",
  lower(consent_id)::text as "access_policy_code"
from (select distinct consent_id from {{ ref('cser_kidscanseq_stg_subject') }}) as subject