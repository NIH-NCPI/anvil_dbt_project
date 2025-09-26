




    
    

select distinct
  filename::text as "filename",
  format::text as "format",
  data_type::text as "data_type",
  size::bigint as "size",
  drs_uri::text as "drs_uri",
  file_metadata::text as "file_metadata",
  has_access_policy::text as "has_access_policy",
  id::text as "id"
from "dbt"."main_main"."fallback_table"
