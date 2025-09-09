

    with source as (
        select 
        GEN_UNKNOWN.age_at_collection::integer as "age_at_collection",
       GEN_UNKNOWN.method::text as "method",
       GEN_UNKNOWN.site::text as "site",
       GEN_UNKNOWN.spatial_qualifier::text as "spatial_qualifier",
       GEN_UNKNOWN.laterality::text as "laterality",
       '' || '_' || md5('GREGoR_R03_GRU_20250612' || '|' || cast(coalesce(, '') as text))::text as "has_access_policy",
       '' || '_' || md5('GREGoR_R03_GRU_20250612' || '|' || cast(coalesce(, '') as text))::text as "id"
        from "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_participant" as participant
        join "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source