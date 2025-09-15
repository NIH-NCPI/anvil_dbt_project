

    with source as (
        select 
        '' || '_' || md5('GREGoR_R03_HMB_20250612' || '|' || cast(coalesce(, '') as text))::text as "filemetadata_id",
       GEN_UNKNOWN.external_id::text as "external_id"
        from "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_participant" as participant
        join "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source