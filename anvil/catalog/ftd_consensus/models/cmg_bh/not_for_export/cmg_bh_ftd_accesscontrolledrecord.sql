{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  {{ generate_global_id(prefix='ap',descriptor=['ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='ac',descriptor=['ingest_provenance'], study_id='cmg_bh') }}::text as "id"
from (select distinct ingest_provenance from {{ ref('cmg_bh_stg_subject') }}) as s 