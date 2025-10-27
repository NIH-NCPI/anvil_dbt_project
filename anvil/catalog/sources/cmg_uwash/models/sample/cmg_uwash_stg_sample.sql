{{ config(materialized='table') }}

with source as (
    SELECT
        NULL AS "bam",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        NULL AS "sample_source",
        "sequencing_center"::TEXT AS "sequencing_center"
        ,
        "subject_id"::TEXT AS "subject_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sample_ANVIL_CMG_UWash_DS_CHDEF_20250224_ANV5_202502241753_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sequencing_center': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        NULL AS "bam",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        "sample_source"::TEXT AS "sample_source"
        ,
        "sequencing_center"::TEXT AS "sequencing_center"
        ,
        "subject_id"::TEXT AS "subject_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sample_ANVIL_CMG_UWash_GRU_IRB_20250224_ANV5_202502241723_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sample_source': 'VARCHAR'
        ,'sequencing_center': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        NULL AS "bam",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        "sample_source"::TEXT AS "sample_source"
        ,
        "sequencing_center"::TEXT AS "sequencing_center"
        ,
        "subject_id"::TEXT AS "subject_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sample_ANVIL_CMG_UWash_GRU_20250224_ANV5_202502241706_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sequencing_center': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'sample_source': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        NULL AS "bam",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        NULL AS "sample_source",
        "sequencing_center"::TEXT AS "sequencing_center"
        ,
        "subject_id"::TEXT AS "subject_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sample_ANVIL_CMG_UWASH_HMB_IRB_20250219_ANV5_202502201921_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sequencing_center': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        NULL AS "bam",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        "sample_source"::TEXT AS "sample_source"
        ,
        "sequencing_center"::TEXT AS "sequencing_center"
        ,
        "subject_id"::TEXT AS "subject_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sample_ANVIL_CMG_UWASH_HMB_20250219_ANV5_202502201916_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sample_source': 'VARCHAR'
        ,'sequencing_center': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "bam"::TEXT AS "bam"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        "sample_source"::TEXT AS "sample_source"
        ,
        "sequencing_center"::TEXT AS "sequencing_center"
        ,
        "subject_id"::TEXT AS "subject_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sample_ANVIL_CMG_UWASH_DS_NBIA_20250206_ANV5_202502201903_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'bam': 'VARCHAR'
        ,'sample_source': 'VARCHAR'
        ,'sequencing_center': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        NULL AS "bam",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        NULL AS "sample_source",
        "sequencing_center"::TEXT AS "sequencing_center"
        ,
        "subject_id"::TEXT AS "subject_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sample_ANVIL_CMG_UWASH_DS_HFA_20250206_ANV5_202502201859_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sequencing_center': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        NULL AS "bam",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        "sample_source"::TEXT AS "sample_source"
        ,
        "sequencing_center"::TEXT AS "sequencing_center"
        ,
        "subject_id"::TEXT AS "subject_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sample_ANVIL_CMG_UWash_DS_EP_20250219_ANV5_202502201854_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sample_source': 'VARCHAR'
        ,'sequencing_center': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        NULL AS "bam",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        NULL AS "sample_source",
        "sequencing_center"::TEXT AS "sequencing_center"
        ,
        "subject_id"::TEXT AS "subject_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sample_ANVIL_CMG_UWASH_DS_BDIS_20250206_ANV5_202502201850_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sequencing_center': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        NULL AS "bam",
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        "sample_source"::TEXT AS "sample_source"
        ,
        "sequencing_center"::TEXT AS "sequencing_center"
        ,
        "subject_id"::TEXT AS "subject_id"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sample_ANVIL_CMG_UWASH_DS_BAV_IRB_PUB_RD_20250206_ANV5_202502201846_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sequencing_center': 'VARCHAR'
        ,'subject_id': 'VARCHAR'
        ,'sample_source': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*,
  REPLACE(REPLACE(REPLACE(UPPER(ingest_provenance), 'SAMPLE_ANVIL_CMG_UWASH_', ''),'.TSV',''),'_','-') AS "consent_id",
from source
