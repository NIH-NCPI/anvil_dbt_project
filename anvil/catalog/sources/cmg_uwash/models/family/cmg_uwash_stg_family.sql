{{ config(materialized='table') }}

with source as (
      SELECT
        "consanguinity"::TEXT AS "consanguinity"
        ,
        NULL AS "consanguinity_detail",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "family_history",
        "family_id"::TEXT AS "family_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        NULL AS "pedigree_detail",
        NULL AS "pedigree_image"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/family_ANVIL_CMG_UWash_DS_CHDEF_20250224_ANV5_202502241753_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'consanguinity': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consanguinity"::TEXT AS "consanguinity"
        ,
        NULL AS "consanguinity_detail",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "family_history",
        "family_id"::TEXT AS "family_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        NULL AS "pedigree_detail",
        NULL AS "pedigree_image"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/family_ANVIL_CMG_UWash_GRU_IRB_20250224_ANV5_202502241723_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'consanguinity': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consanguinity"::TEXT AS "consanguinity"
        ,
        "consanguinity_detail"::TEXT AS "consanguinity_detail"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "family_history"::TEXT AS "family_history"
        ,
        "family_id"::TEXT AS "family_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "pedigree_detail"::TEXT AS "pedigree_detail"
        ,
        "pedigree_image"::TEXT AS "pedigree_image"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/family_ANVIL_CMG_UWash_GRU_20250224_ANV5_202502241706_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'consanguinity': 'VARCHAR'
        ,'family_history': 'VARCHAR'
        ,'pedigree_detail': 'VARCHAR'
        ,'pedigree_image': 'VARCHAR'
        ,'consanguinity_detail': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consanguinity"::TEXT AS "consanguinity"
        ,
        NULL AS "consanguinity_detail",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "family_history",
        "family_id"::TEXT AS "family_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        NULL AS "pedigree_detail",
        NULL AS "pedigree_image"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/family_ANVIL_CMG_UWASH_HMB_IRB_20250219_ANV5_202502201921_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'consanguinity': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consanguinity"::TEXT AS "consanguinity"
        ,
        "consanguinity_detail"::TEXT AS "consanguinity_detail"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "family_history"::TEXT AS "family_history"
        ,
        "family_id"::TEXT AS "family_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        NULL AS "pedigree_detail",
        NULL AS "pedigree_image"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/family_ANVIL_CMG_UWASH_HMB_20250219_ANV5_202502201916_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'consanguinity': 'VARCHAR'
        ,'family_history': 'VARCHAR'
        ,'consanguinity_detail': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consanguinity"::TEXT AS "consanguinity"
        ,
        NULL AS "consanguinity_detail",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "family_history",
        "family_id"::TEXT AS "family_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        NULL AS "pedigree_detail",
        NULL AS "pedigree_image"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/family_ANVIL_CMG_UWASH_DS_NBIA_20250206_ANV5_202502201903_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'consanguinity': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        NULL AS "consanguinity",
        NULL AS "consanguinity_detail",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "family_history",
        "family_id"::TEXT AS "family_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        NULL AS "pedigree_detail",
        NULL AS "pedigree_image"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/family_ANVIL_CMG_UWASH_DS_HFA_20250206_ANV5_202502201859_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consanguinity"::TEXT AS "consanguinity"
        ,
        NULL AS "consanguinity_detail",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "family_history",
        "family_id"::TEXT AS "family_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        NULL AS "pedigree_detail",
        NULL AS "pedigree_image"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/family_ANVIL_CMG_UWash_DS_EP_20250219_ANV5_202502201854_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'consanguinity': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consanguinity"::TEXT AS "consanguinity"
        ,
        NULL AS "consanguinity_detail",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "family_history",
        "family_id"::TEXT AS "family_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        NULL AS "pedigree_detail",
        NULL AS "pedigree_image"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/family_ANVIL_CMG_UWASH_DS_BDIS_20250206_ANV5_202502201850_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'consanguinity': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consanguinity"::TEXT AS "consanguinity"
        ,
        NULL AS "consanguinity_detail",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        NULL AS "family_history",
        "family_id"::TEXT AS "family_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        NULL AS "pedigree_detail",
        NULL AS "pedigree_image"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/family_ANVIL_CMG_UWASH_DS_BAV_IRB_PUB_RD_20250206_ANV5_202502201846_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'family_id': 'VARCHAR'
        ,'consanguinity': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*,
  REPLACE(REPLACE(UPPER(ingest_provenance), 'FAMILY_ANVIL_CMG_UWASH_', ''),'.TSV','') AS "consent_id",
from source
