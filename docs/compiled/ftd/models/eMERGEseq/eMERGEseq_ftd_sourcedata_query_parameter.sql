

    with source as (
        select 
        '' || '_' || md5('eMERGEseq' || '|' || cast(coalesce(, '') as text))::text as "sourcedata_id",
       GEN_UNKNOWN.query_parameter::text as "query_parameter"
        from "dbt"."main_main"."eMERGEseq_stg_demographics" as demographics
        join "dbt"."main_main"."eMERGEseq_stg_phecode" as phecode
on demographics.subject_id = phecode.subject_id  join "dbt"."main_main"."eMERGEseq_stg_sampleattributes" as sampleattributes
on samplesubjectmapping.sample_id = sampleattributes.sample_id  join "dbt"."main_main"."eMERGEseq_stg_samplesubjectmapping" as samplesubjectmapping
on sampleattributes.sample_id = samplesubjectmapping.sample_id  join "dbt"."main_main"."eMERGEseq_stg_subjectconsent" as subjectconsent
on   join "dbt"."main_main"."eMERGEseq_stg_bmi" as bmi
on   join "dbt"."main_main"."eMERGEseq_stg_pedigree" as pedigree
on  
    )

    select 
        * 
    from source