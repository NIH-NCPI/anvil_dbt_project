

    with source as (
        select 
        GEN_UNKNOWN.code::text as "code",
       GEN_UNKNOWN.display::text as "display",
       GEN_UNKNOWN.value_code::text as "value_code",
       GEN_UNKNOWN.value_display::text as "value_display",
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