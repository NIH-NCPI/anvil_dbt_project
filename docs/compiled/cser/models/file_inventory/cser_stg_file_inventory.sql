

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
       "file_ref"::text as "file_ref",
       "ingest_provenance"::text as "ingest_provenance"
    from "dbt"."main"."cser_file_inventory"
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*,
  'GRU' as "consent_id"
from source