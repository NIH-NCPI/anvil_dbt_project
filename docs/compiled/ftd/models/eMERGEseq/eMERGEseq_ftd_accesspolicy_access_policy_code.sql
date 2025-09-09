

    with source as (
        select DISTINCT
        'ap' || '_' || md5('phs001616' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
       'gru' as "access_policy_code"
        from "dbt"."main_main"."eMERGEseq_stg_subjectconsent" as subjectconsent
          WHERE subjectconsent.consent IN ('1', '3', '4', '5', '9')

    UNION ALL
        
    select DISTINCT
        'ap' || '_' || md5('phs001616' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
        'irb' as "access_policy_code"
    from "dbt"."main_main"."eMERGEseq_stg_subjectconsent" as subjectconsent
    WHERE subjectconsent.consent IN ('3', '4', '5', '10')

    UNION ALL
        
    select DISTINCT
        'ap' || '_' || md5('phs001616' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
        'npu' as "access_policy_code"
    from "dbt"."main_main"."eMERGEseq_stg_subjectconsent" as subjectconsent
    WHERE subjectconsent.consent IN ('4', '5', '8', '9')

     UNION ALL
        
    select DISTINCT
        'ap' || '_' || md5('phs001616' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
        'pub' as "access_policy_code"
    from "dbt"."main_main"."eMERGEseq_stg_subjectconsent" as subjectconsent
    WHERE subjectconsent.consent IN ('5', '10')
    
    UNION ALL
        
    select DISTINCT
        'ap' || '_' || md5('phs001616' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
        'hmb' as "access_policy_code"
    from "dbt"."main_main"."eMERGEseq_stg_subjectconsent" as subjectconsent
    WHERE subjectconsent.consent IN ('6', '7', '8', '10')

    UNION ALL
        
    select DISTINCT
        'ap' || '_' || md5('phs001616' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
        'gso' as "access_policy_code"
    from "dbt"."main_main"."eMERGEseq_stg_subjectconsent" as subjectconsent
    WHERE subjectconsent.consent IN ('7')
    )

    select 
        * 
    from source