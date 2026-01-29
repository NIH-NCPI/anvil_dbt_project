{{ config(materialized='table') }}

with source as (
    select 
      "file_id"::text as "file_id",
       "name"::text as "name",
       "path"::text as "path",
       "uri"::text as "uri",
       "content_type"::text as "content_type",
       "full_extension"::text as "full_extension",
       "size_in_bytes"::text as "size_in_bytes",
       "crc32c"::text as "crc32c",
       "md5_hash"::text as "md5_hash",
       "file_ref"::text as "file_ref"
    from {{ source('alscompute','alscompute_hmb_file_inventory') }}
    
    union all
    
    select 
      "file_id"::text as "file_id",
       "name"::text as "name",
       "path"::text as "path",
       "uri"::text as "uri",
       "content_type"::text as "content_type",
       "full_extension"::text as "full_extension",
       "size_in_bytes"::text as "size_in_bytes",
       "crc32c"::text as "crc32c",
       "md5_hash"::text as "md5_hash",
       "file_ref"::text as "file_ref"
    from {{ source('alscompute','alscompute_hmb_file_inventory1') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*,
  (select REPLACE(title, 'ANVIL_ALSCompute_Collection_', '')
      from {{ source('alscompute','alscompute_hmb_anvil_dataset') }}
      limit 1
   ) as consent_id
from source
