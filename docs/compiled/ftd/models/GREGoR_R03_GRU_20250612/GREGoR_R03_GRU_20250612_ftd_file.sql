

    with source as (
        select 
        GEN_UNKNOWN.filename::text as "filename",
       GEN_UNKNOWN.format::text as "format",
       GEN_UNKNOWN.data_type::text as "data_type",
       GEN_UNKNOWN.size::integer as "size",
       GEN_UNKNOWN.drs_uri::text as "drs_uri",
       '' || '_' || md5('GREGoR_R03_GRU_20250612' || '|' || cast(coalesce(, '') as text))::text as "file_metadata",
       '' || '_' || md5('GREGoR_R03_GRU_20250612' || '|' || cast(coalesce(, '') as text))::text as "has_access_policy",
       '' || '_' || md5('GREGoR_R03_GRU_20250612' || '|' || cast(coalesce(, '') as text))::text as "id"
        from "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_participant" as participant
        join "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source