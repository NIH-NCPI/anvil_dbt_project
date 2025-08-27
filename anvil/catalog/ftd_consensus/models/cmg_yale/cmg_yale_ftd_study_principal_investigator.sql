{{ config(materialized='table', schema='cmg_yale_data') }}

select 
  {{ generate_global_id(prefix='st',descriptor=['dbgap_study_id'], study_id='cmg_yale') }}::text as "study_id",
  anvil_dataset.principal_investigator::text as "principal_investigator"
from (
    (select distinct dbgap_study_id from {{ ref('cmg_yale_stg_subject') }} where dbgap_study_id is not null) as subject
   left join 
    (select distinct principal_investigator, registered_identifier from {{ ref('cmg_yale_stg_anvil_dataset') }}) as anvil_dataset
   on subject.dbgap_study_id = anvil_dataset.registered_identifier
) as s