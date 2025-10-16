{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
  GEN_UNKNOWN.description::text as "description",
  NULL::text as "website",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_uwash') }}::text as "id"
from  {{ ref('cmg_uwash_stg_anvil_dataset') }} as ad
