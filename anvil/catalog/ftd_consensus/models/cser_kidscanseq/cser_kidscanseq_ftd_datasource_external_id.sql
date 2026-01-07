{{ config(materialized='table', schema='cser_kidscanseq_data') }}

select 
  {{ generate_global_id(prefix='ds',descriptor=['registered_identifier'], study_id='cser_kidscanseq') }}::text as "datasource_id",
  registered_identifier::text as "external_id"
from (select distinct registered_identifier from {{ ref('cser_kidscanseq_stg_anvil_dataset') }}) as anvil_dataset
