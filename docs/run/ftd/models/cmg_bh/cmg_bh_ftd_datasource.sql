
  
    
    

    create  table
      "dbt"."main_cmg_bh_data"."cmg_bh_ftd_datasource__dbt_tmp"
  
    as (
      

select 
  '07b0243c-48fc-4eee-a338-c7571cc2df1a'::text as "snapshot_id",
  'datarepo-d321333c'::text as "google_data_project",
  'ANVIL_CMG_BaylorHopkins_HMB_NPU_WES_20230525_ANV5_202402290537'::text as "snapshot_dataset",
  NULL::text as "table",
  NULL::text as "parameterized_query",
  'ds' || '_' || md5('cmg_bh' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "id"
from (select distinct dbgap_study_id from "dbt"."main_main"."cmg_bh_stg_subject" ) as s
    );
  
  