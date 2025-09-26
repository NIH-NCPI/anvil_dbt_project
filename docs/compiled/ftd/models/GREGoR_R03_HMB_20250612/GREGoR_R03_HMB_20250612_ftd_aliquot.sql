

    with source as (
        select 
        GEN_UNKNOWN.availablity_status::text as "availablity_status",
       GEN_UNKNOWN.quantity_number::text as "quantity_number",
       GEN_UNKNOWN.quantity_units::text as "quantity_units",
       GEN_UNKNOWN.concentration_number::text as "concentration_number",
       GEN_UNKNOWN.concentration_unit::text as "concentration_unit",
       '' || '_' || md5('GREGoR_R03_HMB_20250612' || '|' || cast(coalesce(, '') as text))::text as "has_access_policy",
       '' || '_' || md5('GREGoR_R03_HMB_20250612' || '|' || cast(coalesce(, '') as text))::text as "id",
       '' || '_' || md5('GREGoR_R03_HMB_20250612' || '|' || cast(coalesce(, '') as text))::text as "sample_id"
        from "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_participant" as participant
        join "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source