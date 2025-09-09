

    with source as (
        select DISTINCT
        'fm' || '_' || md5('phs003047' || '|' || cast(coalesce(participant.participant_id, '') as text))::text as "family_member",
        CASE participant.proband_relationship
            WHEN 'Mother' THEN 'MTH'
            WHEN 'Father' THEN 'FTH'
            WHEN 'Sibling' THEN 'SIB'
            WHEN 'Child' THEN 'CHILD'
            WHEN 'Maternal Half Sibling' THEN 'HSIB'
            WHEN 'Paternal Half Sibling' THEN 'HSIB'
            WHEN 'Maternal Grandmother' THEN 'MGRMTH'
            WHEN 'Maternal Grandfather' THEN 'MGRFTH'
            WHEN 'Paternal Grandmother' THEN 'PGRMTH'
            WHEN 'Paternal Grandfather' THEN 'PGRFTH'
            WHEN 'Maternal Aunt' THEN 'MAUNT'
            WHEN 'Paternal Aunt' THEN 'PAUNT'
            WHEN 'Maternal Uncle' THEN 'MUNCLE'
            WHEN 'Paternal Uncle' THEN 'PUNCLE'
            WHEN 'Niece' THEN 'NIECE'
            WHEN 'Nephew' THEN 'NEPHEW'
            WHEN 'Maternal 1st Cousin' THEN 'MCOUSN'
            WHEN 'Paternal 1st Cousin' THEN 'PCOUSN'
            WHEN 'Self' THEN 'SNOMED:85900004'
        END::text as "family_role",       
        'ap' || '_' || md5('phs003047' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "has_access_policy",
        'fm' || '_' || md5('phs003047' || '|' || cast(coalesce(participant.participant_id, '') as text) || '|' || 'phs003047' || '|' || cast(coalesce(participant.family_id, '') as text))::text as "id",
       'fy' || '_' || md5('phs003047' || '|' || cast(coalesce(participant.family_id, '') as text))::text as "family_id"
        from "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_participant" as participant
    )

    select 
        * 
    from source