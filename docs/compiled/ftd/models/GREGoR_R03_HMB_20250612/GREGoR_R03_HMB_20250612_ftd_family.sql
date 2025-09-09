

    with source as (
        select 
        GEN_UNKNOWN.family_type::text as "family_type",
       GEN_UNKNOWN.family_description::text as "family_description",
       GEN_UNKNOWN.consanguinity::text as "consanguinity",
       GEN_UNKNOWN.family_study_focus::text as "family_study_focus",
       '' || '_' || md5('GREGoR_R03_HMB_20250612' || '|' || cast(coalesce(, '') as text))::text as "has_access_policy",
        -- participant.family_id::text as "id"
       -- Check if using the auto-generated id is correct. 
       '' || '_' || md5('GREGoR_R03_HMB_20250612' || '|' || cast(coalesce(, '') as text))::text as "id"
        from "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_participant" as participant
        join "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_phenotype" as phenotype
        on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source