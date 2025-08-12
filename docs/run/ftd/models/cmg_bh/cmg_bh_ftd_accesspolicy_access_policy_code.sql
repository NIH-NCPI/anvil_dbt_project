
  
    
    

    create  table
      "dbt"."main_cmg_bh_data"."cmg_bh_ftd_accesspolicy_access_policy_code__dbt_tmp"
  
    as (
      select
      'ap' || '_' || md5('cmg_bh' || '|' || cast(coalesce(ingest_provenance, '') as text))::text as "accesspolicy_id",
      'hmb' as access_policy_code,
    from (select distinct ingest_provenance from "dbt"."main_main"."cmg_bh_stg_subject") as s
    where s.ingest_provenance ILIKE 'hmb'
    union all
select
      'ap' || '_' || md5('cmg_bh' || '|' || cast(coalesce(ingest_provenance, '') as text))::text as "accesspolicy_id",
      'irb' as access_policy_code,
    from (select distinct ingest_provenance from "dbt"."main_main"."cmg_bh_stg_subject") as s
    where s.ingest_provenance ILIKE 'irb'
    union all
select
      'ap' || '_' || md5('cmg_bh' || '|' || cast(coalesce(ingest_provenance, '') as text))::text as "accesspolicy_id",
      'npu' as access_policy_code,
    from (select distinct ingest_provenance from "dbt"."main_main"."cmg_bh_stg_subject") as s
    where s.ingest_provenance ILIKE 'npu'
    

    );
  
  