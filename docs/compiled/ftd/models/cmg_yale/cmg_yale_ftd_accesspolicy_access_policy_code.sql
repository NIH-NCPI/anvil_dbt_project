select
      'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "accesspolicy_id",
      'gru' as access_policy_code,
      consent_id as "ftd_consent_group"
    from (select distinct consent_id from "dbt"."main_main"."cmg_yale_stg_subject") as s
    where s.consent_id ILIKE '%gru%'
    union all
select
      'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "accesspolicy_id",
      'ds' as access_policy_code,
      consent_id as "ftd_consent_group"
    from (select distinct consent_id from "dbt"."main_main"."cmg_yale_stg_subject") as s
    where s.consent_id ILIKE '%ds%'
    union all
select
      'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "accesspolicy_id",
      'gso' as access_policy_code,
      consent_id as "ftd_consent_group"
    from (select distinct consent_id from "dbt"."main_main"."cmg_yale_stg_subject") as s
    where s.consent_id ILIKE '%gso%'
    union all
select
      'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "accesspolicy_id",
      'hmb' as access_policy_code,
      consent_id as "ftd_consent_group"
    from (select distinct consent_id from "dbt"."main_main"."cmg_yale_stg_subject") as s
    where s.consent_id ILIKE '%hmb%'
    union all
select
      'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "accesspolicy_id",
      'irb' as access_policy_code,
      consent_id as "ftd_consent_group"
    from (select distinct consent_id from "dbt"."main_main"."cmg_yale_stg_subject") as s
    where s.consent_id ILIKE '%irb%'
    union all
select
      'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "accesspolicy_id",
      'pub' as access_policy_code,
      consent_id as "ftd_consent_group"
    from (select distinct consent_id from "dbt"."main_main"."cmg_yale_stg_subject") as s
    where s.consent_id ILIKE '%pub%'
    union all
select
      'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "accesspolicy_id",
      'col' as access_policy_code,
      consent_id as "ftd_consent_group"
    from (select distinct consent_id from "dbt"."main_main"."cmg_yale_stg_subject") as s
    where s.consent_id ILIKE '%col%'
    union all
select
      'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "accesspolicy_id",
      'npu' as access_policy_code,
      consent_id as "ftd_consent_group"
    from (select distinct consent_id from "dbt"."main_main"."cmg_yale_stg_subject") as s
    where s.consent_id ILIKE '%npu%'
    union all
select
      'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "accesspolicy_id",
      'mds' as access_policy_code,
      consent_id as "ftd_consent_group"
    from (select distinct consent_id from "dbt"."main_main"."cmg_yale_stg_subject") as s
    where s.consent_id ILIKE '%mds%'
    union all
select
      'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "accesspolicy_id",
      'gsr' as access_policy_code,
      consent_id as "ftd_consent_group"
    from (select distinct consent_id from "dbt"."main_main"."cmg_yale_stg_subject") as s
    where s.consent_id ILIKE '%gsr%'
    
