{{ config(materialized='table') }}

with source as (
  SELECT
        "consent_group"::TEXT AS "consent_group"
        ,
        "data_modality"::TEXT AS "data_modality"
        ,
        "data_use_permission"::TEXT AS "data_use_permission"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "dataset_id"::TEXT AS "dataset_id"
        ,
        "owner"::TEXT AS "owner"
        ,
        "principal_investigator"::TEXT AS "principal_investigator"
        ,
        "registered_identifier"::TEXT AS "registered_identifier"
        ,
        "source_datarepo_row_ids"::TEXT AS "source_datarepo_row_ids"
        ,
        "title"::TEXT AS "title"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/anvil_dataset_ANVIL_CMG_UWash_DS_CHDEF_20250224_ANV5_202502241753_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'dataset_id': 'VARCHAR'
        ,'consent_group': 'VARCHAR'
        ,'data_use_permission': 'VARCHAR'
        ,'owner': 'VARCHAR'
        ,'principal_investigator': 'VARCHAR'
        ,'registered_identifier': 'VARCHAR'
        ,'title': 'VARCHAR'
        ,'data_modality': 'VARCHAR'
        ,'source_datarepo_row_ids': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consent_group"::TEXT AS "consent_group"
        ,
        "data_modality"::TEXT AS "data_modality"
        ,
        "data_use_permission"::TEXT AS "data_use_permission"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "dataset_id"::TEXT AS "dataset_id"
        ,
        "owner"::TEXT AS "owner"
        ,
        "principal_investigator"::TEXT AS "principal_investigator"
        ,
        "registered_identifier"::TEXT AS "registered_identifier"
        ,
        "source_datarepo_row_ids"::TEXT AS "source_datarepo_row_ids"
        ,
        "title"::TEXT AS "title"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/anvil_dataset_ANVIL_CMG_UWash_GRU_IRB_20250224_ANV5_202502241723_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'dataset_id': 'VARCHAR'
        ,'consent_group': 'VARCHAR'
        ,'data_use_permission': 'VARCHAR'
        ,'owner': 'VARCHAR'
        ,'principal_investigator': 'VARCHAR'
        ,'registered_identifier': 'VARCHAR'
        ,'title': 'VARCHAR'
        ,'data_modality': 'VARCHAR'
        ,'source_datarepo_row_ids': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consent_group"::TEXT AS "consent_group"
        ,
        "data_modality"::TEXT AS "data_modality"
        ,
        "data_use_permission"::TEXT AS "data_use_permission"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "dataset_id"::TEXT AS "dataset_id"
        ,
        "owner"::TEXT AS "owner"
        ,
        "principal_investigator"::TEXT AS "principal_investigator"
        ,
        "registered_identifier"::TEXT AS "registered_identifier"
        ,
        "source_datarepo_row_ids"::TEXT AS "source_datarepo_row_ids"
        ,
        "title"::TEXT AS "title"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/anvil_dataset_ANVIL_CMG_UWash_GRU_20250224_ANV5_202502241706_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'dataset_id': 'VARCHAR'
        ,'consent_group': 'VARCHAR'
        ,'data_use_permission': 'VARCHAR'
        ,'owner': 'VARCHAR'
        ,'principal_investigator': 'VARCHAR'
        ,'registered_identifier': 'VARCHAR'
        ,'title': 'VARCHAR'
        ,'data_modality': 'VARCHAR'
        ,'source_datarepo_row_ids': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consent_group"::TEXT AS "consent_group"
        ,
        "data_modality"::TEXT AS "data_modality"
        ,
        "data_use_permission"::TEXT AS "data_use_permission"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "dataset_id"::TEXT AS "dataset_id"
        ,
        "owner"::TEXT AS "owner"
        ,
        "principal_investigator"::TEXT AS "principal_investigator"
        ,
        "registered_identifier"::TEXT AS "registered_identifier"
        ,
        "source_datarepo_row_ids"::TEXT AS "source_datarepo_row_ids"
        ,
        "title"::TEXT AS "title"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/anvil_dataset_ANVIL_CMG_UWASH_HMB_IRB_20250219_ANV5_202502201921_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'dataset_id': 'VARCHAR'
        ,'consent_group': 'VARCHAR'
        ,'data_use_permission': 'VARCHAR'
        ,'owner': 'VARCHAR'
        ,'principal_investigator': 'VARCHAR'
        ,'registered_identifier': 'VARCHAR'
        ,'title': 'VARCHAR'
        ,'data_modality': 'VARCHAR'
        ,'source_datarepo_row_ids': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consent_group"::TEXT AS "consent_group"
        ,
        "data_modality"::TEXT AS "data_modality"
        ,
        "data_use_permission"::TEXT AS "data_use_permission"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "dataset_id"::TEXT AS "dataset_id"
        ,
        "owner"::TEXT AS "owner"
        ,
        "principal_investigator"::TEXT AS "principal_investigator"
        ,
        "registered_identifier"::TEXT AS "registered_identifier"
        ,
        "source_datarepo_row_ids"::TEXT AS "source_datarepo_row_ids"
        ,
        "title"::TEXT AS "title"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/anvil_dataset_ANVIL_CMG_UWASH_HMB_20250219_ANV5_202502201916_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'dataset_id': 'VARCHAR'
        ,'consent_group': 'VARCHAR'
        ,'data_use_permission': 'VARCHAR'
        ,'owner': 'VARCHAR'
        ,'principal_investigator': 'VARCHAR'
        ,'registered_identifier': 'VARCHAR'
        ,'title': 'VARCHAR'
        ,'data_modality': 'VARCHAR'
        ,'source_datarepo_row_ids': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consent_group"::TEXT AS "consent_group"
        ,
        "data_modality"::TEXT AS "data_modality"
        ,
        "data_use_permission"::TEXT AS "data_use_permission"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "dataset_id"::TEXT AS "dataset_id"
        ,
        "owner"::TEXT AS "owner"
        ,
        "principal_investigator"::TEXT AS "principal_investigator"
        ,
        "registered_identifier"::TEXT AS "registered_identifier"
        ,
        "source_datarepo_row_ids"::TEXT AS "source_datarepo_row_ids"
        ,
        "title"::TEXT AS "title"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/anvil_dataset_ANVIL_CMG_UWASH_DS_NBIA_20250206_ANV5_202502201903_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'dataset_id': 'VARCHAR'
        ,'consent_group': 'VARCHAR'
        ,'data_use_permission': 'VARCHAR'
        ,'owner': 'VARCHAR'
        ,'principal_investigator': 'VARCHAR'
        ,'registered_identifier': 'VARCHAR'
        ,'title': 'VARCHAR'
        ,'data_modality': 'VARCHAR'
        ,'source_datarepo_row_ids': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consent_group"::TEXT AS "consent_group"
        ,
        "data_modality"::TEXT AS "data_modality"
        ,
        "data_use_permission"::TEXT AS "data_use_permission"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "dataset_id"::TEXT AS "dataset_id"
        ,
        "owner"::TEXT AS "owner"
        ,
        "principal_investigator"::TEXT AS "principal_investigator"
        ,
        "registered_identifier"::TEXT AS "registered_identifier"
        ,
        "source_datarepo_row_ids"::TEXT AS "source_datarepo_row_ids"
        ,
        "title"::TEXT AS "title"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/anvil_dataset_ANVIL_CMG_UWASH_DS_HFA_20250206_ANV5_202502201859_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'dataset_id': 'VARCHAR'
        ,'consent_group': 'VARCHAR'
        ,'data_use_permission': 'VARCHAR'
        ,'owner': 'VARCHAR'
        ,'principal_investigator': 'VARCHAR'
        ,'registered_identifier': 'VARCHAR'
        ,'title': 'VARCHAR'
        ,'data_modality': 'VARCHAR'
        ,'source_datarepo_row_ids': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consent_group"::TEXT AS "consent_group"
        ,
        "data_modality"::TEXT AS "data_modality"
        ,
        "data_use_permission"::TEXT AS "data_use_permission"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "dataset_id"::TEXT AS "dataset_id"
        ,
        "owner"::TEXT AS "owner"
        ,
        "principal_investigator"::TEXT AS "principal_investigator"
        ,
        "registered_identifier"::TEXT AS "registered_identifier"
        ,
        "source_datarepo_row_ids"::TEXT AS "source_datarepo_row_ids"
        ,
        "title"::TEXT AS "title"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/anvil_dataset_ANVIL_CMG_UWash_DS_EP_20250219_ANV5_202502201854_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'dataset_id': 'VARCHAR'
        ,'consent_group': 'VARCHAR'
        ,'data_use_permission': 'VARCHAR'
        ,'owner': 'VARCHAR'
        ,'principal_investigator': 'VARCHAR'
        ,'registered_identifier': 'VARCHAR'
        ,'title': 'VARCHAR'
        ,'data_modality': 'VARCHAR'
        ,'source_datarepo_row_ids': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consent_group"::TEXT AS "consent_group"
        ,
        "data_modality"::TEXT AS "data_modality"
        ,
        "data_use_permission"::TEXT AS "data_use_permission"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "dataset_id"::TEXT AS "dataset_id"
        ,
        "owner"::TEXT AS "owner"
        ,
        "principal_investigator"::TEXT AS "principal_investigator"
        ,
        "registered_identifier"::TEXT AS "registered_identifier"
        ,
        "source_datarepo_row_ids"::TEXT AS "source_datarepo_row_ids"
        ,
        "title"::TEXT AS "title"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/anvil_dataset_ANVIL_CMG_UWASH_DS_BDIS_20250206_ANV5_202502201850_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'dataset_id': 'VARCHAR'
        ,'consent_group': 'VARCHAR'
        ,'data_use_permission': 'VARCHAR'
        ,'owner': 'VARCHAR'
        ,'principal_investigator': 'VARCHAR'
        ,'registered_identifier': 'VARCHAR'
        ,'title': 'VARCHAR'
        ,'data_modality': 'VARCHAR'
        ,'source_datarepo_row_ids': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "consent_group"::TEXT AS "consent_group"
        ,
        "data_modality"::TEXT AS "data_modality"
        ,
        "data_use_permission"::TEXT AS "data_use_permission"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "dataset_id"::TEXT AS "dataset_id"
        ,
        "owner"::TEXT AS "owner"
        ,
        "principal_investigator"::TEXT AS "principal_investigator"
        ,
        "registered_identifier"::TEXT AS "registered_identifier"
        ,
        "source_datarepo_row_ids"::TEXT AS "source_datarepo_row_ids"
        ,
        "title"::TEXT AS "title"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/anvil_dataset_ANVIL_CMG_UWASH_DS_BAV_IRB_PUB_RD_20250206_ANV5_202502201846_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'dataset_id': 'VARCHAR'
        ,'consent_group': 'VARCHAR'
        ,'data_use_permission': 'VARCHAR'
        ,'owner': 'VARCHAR'
        ,'principal_investigator': 'VARCHAR'
        ,'registered_identifier': 'VARCHAR'
        ,'title': 'VARCHAR'
        ,'data_modality': 'VARCHAR'
        ,'source_datarepo_row_ids': 'VARCHAR'
        })
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source
