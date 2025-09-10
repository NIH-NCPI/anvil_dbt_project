

    with source as (
        select 
        '' || '_' || md5('eMERGEseq' || '|' || cast(coalesce(, '') as text))::text as "demographics_id",
       '' || '_' || md5('eMERGEseq' || '|' || cast(coalesce(, '') as text))::text as "source_data_id"
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