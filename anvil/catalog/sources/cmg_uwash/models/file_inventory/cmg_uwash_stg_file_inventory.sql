{{ config(materialized='table') }}

with source as (
   SELECT
        "content_type"::TEXT AS "content_type"
        ,
        "crc32c"::TEXT AS "crc32c"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "file_id"::TEXT AS "file_id"
        ,
        "file_ref"::TEXT AS "file_ref"
        ,
        "full_extension"::TEXT AS "full_extension"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "md5_hash"::TEXT AS "md5_hash"
        ,
        "name"::TEXT AS "name"
        ,
        "path"::TEXT AS "path"
        ,
        "size_in_bytes"::TEXT AS "size_in_bytes"
        ,
        "uri"::TEXT AS "uri",
        'DS-CHDEF'::TEXT AS "consent_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/file_inventory_ANVIL_CMG_UWash_DS_CHDEF_20250224_ANV5_202502241753_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'file_id': 'VARCHAR'
        ,'name': 'VARCHAR'
        ,'path': 'VARCHAR'
        ,'uri': 'VARCHAR'
        ,'content_type': 'VARCHAR'
        ,'full_extension': 'VARCHAR'
        ,'size_in_bytes': 'VARCHAR'
        ,'crc32c': 'VARCHAR'
        ,'md5_hash': 'VARCHAR'
        ,'file_ref': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "content_type"::TEXT AS "content_type"
        ,
        "crc32c"::TEXT AS "crc32c"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "file_id"::TEXT AS "file_id"
        ,
        "file_ref"::TEXT AS "file_ref"
        ,
        "full_extension"::TEXT AS "full_extension"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "md5_hash"::TEXT AS "md5_hash"
        ,
        "name"::TEXT AS "name"
        ,
        "path"::TEXT AS "path"
        ,
        "size_in_bytes"::TEXT AS "size_in_bytes"
        ,
        "uri"::TEXT AS "uri"
        ,
        'GRU-IRB'::TEXT AS "consent_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/file_inventory_ANVIL_CMG_UWash_GRU_IRB_20250224_ANV5_202502241723_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'file_id': 'VARCHAR'
        ,'name': 'VARCHAR'
        ,'path': 'VARCHAR'
        ,'uri': 'VARCHAR'
        ,'content_type': 'VARCHAR'
        ,'full_extension': 'VARCHAR'
        ,'size_in_bytes': 'VARCHAR'
        ,'crc32c': 'VARCHAR'
        ,'md5_hash': 'VARCHAR'
        ,'file_ref': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "content_type"::TEXT AS "content_type"
        ,
        "crc32c"::TEXT AS "crc32c"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "file_id"::TEXT AS "file_id"
        ,
        "file_ref"::TEXT AS "file_ref"
        ,
        "full_extension"::TEXT AS "full_extension"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "md5_hash"::TEXT AS "md5_hash"
        ,
        "name"::TEXT AS "name"
        ,
        "path"::TEXT AS "path"
        ,
        "size_in_bytes"::TEXT AS "size_in_bytes"
        ,
        "uri"::TEXT AS "uri"
        ,
        'GRU'::TEXT AS "consent_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/file_inventory_ANVIL_CMG_UWash_GRU_20250224_ANV5_202502241706_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'file_id': 'VARCHAR'
        ,'name': 'VARCHAR'
        ,'path': 'VARCHAR'
        ,'uri': 'VARCHAR'
        ,'content_type': 'VARCHAR'
        ,'full_extension': 'VARCHAR'
        ,'size_in_bytes': 'VARCHAR'
        ,'crc32c': 'VARCHAR'
        ,'md5_hash': 'VARCHAR'
        ,'file_ref': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "content_type"::TEXT AS "content_type"
        ,
        "crc32c"::TEXT AS "crc32c"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "file_id"::TEXT AS "file_id"
        ,
        "file_ref"::TEXT AS "file_ref"
        ,
        "full_extension"::TEXT AS "full_extension"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "md5_hash"::TEXT AS "md5_hash"
        ,
        "name"::TEXT AS "name"
        ,
        "path"::TEXT AS "path"
        ,
        "size_in_bytes"::TEXT AS "size_in_bytes"
        ,
        "uri"::TEXT AS "uri"
        ,
        'HMB-IRB'::TEXT AS "consent_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/file_inventory_ANVIL_CMG_UWASH_HMB_IRB_20250219_ANV5_202502201921_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'file_id': 'VARCHAR'
        ,'name': 'VARCHAR'
        ,'path': 'VARCHAR'
        ,'uri': 'VARCHAR'
        ,'content_type': 'VARCHAR'
        ,'full_extension': 'VARCHAR'
        ,'size_in_bytes': 'VARCHAR'
        ,'crc32c': 'VARCHAR'
        ,'md5_hash': 'VARCHAR'
        ,'file_ref': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "content_type"::TEXT AS "content_type"
        ,
        "crc32c"::TEXT AS "crc32c"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "file_id"::TEXT AS "file_id"
        ,
        "file_ref"::TEXT AS "file_ref"
        ,
        "full_extension"::TEXT AS "full_extension"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "md5_hash"::TEXT AS "md5_hash"
        ,
        "name"::TEXT AS "name"
        ,
        "path"::TEXT AS "path"
        ,
        "size_in_bytes"::TEXT AS "size_in_bytes"
        ,
        "uri"::TEXT AS "uri"
        ,
        'HMB'::TEXT AS "consent_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/file_inventory_ANVIL_CMG_UWASH_HMB_20250219_ANV5_202502201916_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'file_id': 'VARCHAR'
        ,'name': 'VARCHAR'
        ,'path': 'VARCHAR'
        ,'uri': 'VARCHAR'
        ,'content_type': 'VARCHAR'
        ,'full_extension': 'VARCHAR'
        ,'size_in_bytes': 'VARCHAR'
        ,'crc32c': 'VARCHAR'
        ,'md5_hash': 'VARCHAR'
        ,'file_ref': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "content_type"::TEXT AS "content_type"
        ,
        "crc32c"::TEXT AS "crc32c"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "file_id"::TEXT AS "file_id"
        ,
        "file_ref"::TEXT AS "file_ref"
        ,
        "full_extension"::TEXT AS "full_extension"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "md5_hash"::TEXT AS "md5_hash"
        ,
        "name"::TEXT AS "name"
        ,
        "path"::TEXT AS "path"
        ,
        "size_in_bytes"::TEXT AS "size_in_bytes"
        ,
        "uri"::TEXT AS "uri"
        ,
        'DS-NBIA'::TEXT AS "consent_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/file_inventory_ANVIL_CMG_UWASH_DS_NBIA_20250206_ANV5_202502201903_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'file_id': 'VARCHAR'
        ,'name': 'VARCHAR'
        ,'path': 'VARCHAR'
        ,'uri': 'VARCHAR'
        ,'content_type': 'VARCHAR'
        ,'full_extension': 'VARCHAR'
        ,'size_in_bytes': 'VARCHAR'
        ,'crc32c': 'VARCHAR'
        ,'md5_hash': 'VARCHAR'
        ,'file_ref': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "content_type"::TEXT AS "content_type"
        ,
        "crc32c"::TEXT AS "crc32c"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "file_id"::TEXT AS "file_id"
        ,
        "file_ref"::TEXT AS "file_ref"
        ,
        "full_extension"::TEXT AS "full_extension"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "md5_hash"::TEXT AS "md5_hash"
        ,
        "name"::TEXT AS "name"
        ,
        "path"::TEXT AS "path"
        ,
        "size_in_bytes"::TEXT AS "size_in_bytes"
        ,
        "uri"::TEXT AS "uri"
        ,
        'DS-HFA'::TEXT AS "consent_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/file_inventory_ANVIL_CMG_UWASH_DS_HFA_20250206_ANV5_202502201859_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'file_id': 'VARCHAR'
        ,'name': 'VARCHAR'
        ,'path': 'VARCHAR'
        ,'uri': 'VARCHAR'
        ,'content_type': 'VARCHAR'
        ,'full_extension': 'VARCHAR'
        ,'size_in_bytes': 'VARCHAR'
        ,'crc32c': 'VARCHAR'
        ,'md5_hash': 'VARCHAR'
        ,'file_ref': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "content_type"::TEXT AS "content_type"
        ,
        "crc32c"::TEXT AS "crc32c"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "file_id"::TEXT AS "file_id"
        ,
        "file_ref"::TEXT AS "file_ref"
        ,
        "full_extension"::TEXT AS "full_extension"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "md5_hash"::TEXT AS "md5_hash"
        ,
        "name"::TEXT AS "name"
        ,
        "path"::TEXT AS "path"
        ,
        "size_in_bytes"::TEXT AS "size_in_bytes"
        ,
        "uri"::TEXT AS "uri"
        ,
        'DS-EP'::TEXT AS "consent_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/file_inventory_ANVIL_CMG_UWash_DS_EP_20250219_ANV5_202502201854_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'file_id': 'VARCHAR'
        ,'name': 'VARCHAR'
        ,'path': 'VARCHAR'
        ,'uri': 'VARCHAR'
        ,'content_type': 'VARCHAR'
        ,'full_extension': 'VARCHAR'
        ,'size_in_bytes': 'VARCHAR'
        ,'crc32c': 'VARCHAR'
        ,'md5_hash': 'VARCHAR'
        ,'file_ref': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "content_type"::TEXT AS "content_type"
        ,
        "crc32c"::TEXT AS "crc32c"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "file_id"::TEXT AS "file_id"
        ,
        "file_ref"::TEXT AS "file_ref"
        ,
        "full_extension"::TEXT AS "full_extension"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "md5_hash"::TEXT AS "md5_hash"
        ,
        "name"::TEXT AS "name"
        ,
        "path"::TEXT AS "path"
        ,
        "size_in_bytes"::TEXT AS "size_in_bytes"
        ,
        "uri"::TEXT AS "uri"
        ,
        'DS-BDIS'::TEXT AS "consent_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/file_inventory_ANVIL_CMG_UWASH_DS_BDIS_20250206_ANV5_202502201850_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'file_id': 'VARCHAR'
        ,'name': 'VARCHAR'
        ,'path': 'VARCHAR'
        ,'uri': 'VARCHAR'
        ,'content_type': 'VARCHAR'
        ,'full_extension': 'VARCHAR'
        ,'size_in_bytes': 'VARCHAR'
        ,'crc32c': 'VARCHAR'
        ,'md5_hash': 'VARCHAR'
        ,'file_ref': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "content_type"::TEXT AS "content_type"
        ,
        "crc32c"::TEXT AS "crc32c"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "file_id"::TEXT AS "file_id"
        ,
        "file_ref"::TEXT AS "file_ref"
        ,
        "full_extension"::TEXT AS "full_extension"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "md5_hash"::TEXT AS "md5_hash"
        ,
        "name"::TEXT AS "name"
        ,
        "path"::TEXT AS "path"
        ,
        "size_in_bytes"::TEXT AS "size_in_bytes"
        ,
        "uri"::TEXT AS "uri"
        ,
        'DS-BAV-IRB-PUB-RD'::TEXT AS "consent_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/file_inventory_ANVIL_CMG_UWASH_DS_BAV_IRB_PUB_RD_20250206_ANV5_202502201846_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'file_id': 'VARCHAR'
        ,'name': 'VARCHAR'
        ,'path': 'VARCHAR'
        ,'uri': 'VARCHAR'
        ,'content_type': 'VARCHAR'
        ,'full_extension': 'VARCHAR'
        ,'size_in_bytes': 'VARCHAR'
        ,'crc32c': 'VARCHAR'
        ,'md5_hash': 'VARCHAR'
        ,'file_ref': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source
