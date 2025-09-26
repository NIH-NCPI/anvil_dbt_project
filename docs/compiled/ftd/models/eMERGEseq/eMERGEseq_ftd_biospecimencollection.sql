

    with source as (
        select 
        GEN_UNKNOWN.age_at_collection::integer as "age_at_collection",
       GEN_UNKNOWN.method::text as "method",
       GEN_UNKNOWN.site::text as "site",
       GEN_UNKNOWN.spatial_qualifier::text as "spatial_qualifier",
       GEN_UNKNOWN.laterality::text as "laterality",
       '' || '_' || md5('eMERGEseq' || '|' || cast(coalesce(, '') as text))::text as "has_access_policy",
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