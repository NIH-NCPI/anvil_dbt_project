

    with source as (
        select 
        '' || '_' || md5('eMERGEseq' || '|' || cast(coalesce(, '') as text))::text as "parent_study_id",
       GEN_UNKNOWN.study_title::text as "study_title",
       '' || '_' || md5('eMERGEseq' || '|' || cast(coalesce(, '') as text))::text as "id"
        from "dbt"."main_main"."eMERGEseq_stg_subjectconsent" as subjectconsent
        join "dbt"."main_main"."eMERGEseq_stg_demographics" as demographics
on subjectconsent.subject_id = demographics.subject_id  join "dbt"."main_main"."eMERGEseq_stg_phecode" as phecode
on   join "dbt"."main_main"."eMERGEseq_stg_samplesubjectmapping" as samplesubjectmapping
on   join "dbt"."main_main"."eMERGEseq_stg_sampleattributes" as sampleattributes
on  
    )

    select 
        * 
    from source