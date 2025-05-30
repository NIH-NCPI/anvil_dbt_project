{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        CASE 
            WHEN participant.paternal_id != 0 THEN participant.paternal_id
            WHEN participant.maternal_id != 0 THEN participant.maternal_id
            WHEN participant.twin_id != 0 THEN CAST(participant.twin_id AS INTEGER)
        END::text as "family_member",
        CASE 
            WHEN participant.paternal_id != 0 OR participant.maternal_id != 0 OR participant.twin_id != 0
            THEN participant.AnVIL_GREGoR_GSS_U07_GRU_participant_id
        END::text as "other_family_member",
        CASE 
            WHEN participant.paternal_id != 0 THEN 'KIN:027'
            WHEN participant.maternal_id != 0 THEN 'KIN:028'
            WHEN participant.twin_id != 0 THEN 'KIN:009'
            WHEN participant.proband_relationship =  'Mother' THEN 'MTH'
            WHEN participant.proband_relationship =  'Father' THEN 'FTH'
            WHEN participant.proband_relationship =  'Sibling' THEN 'SIB'
            WHEN participant.proband_relationship =  'Child' THEN 'CHILD'
            WHEN participant.proband_relationship =  'Maternal Half Sibling' THEN 'HSIB'
            WHEN participant.proband_relationship =  'Paternal Half Sibling' THEN 'HSIB'
            WHEN participant.proband_relationship =  'Maternal Grandmother' THEN 'MGRMTH'
            WHEN participant.proband_relationship =  'Maternal Grandfather' THEN 'MGRFTH'
            WHEN participant.proband_relationship =  'Paternal Grandmother' THEN 'PGRMTH'
            WHEN participant.proband_relationship =  'Paternal Grandfather' THEN 'PGRFTH'
            WHEN participant.proband_relationship =  'Maternal Aunt' THEN 'MAUNT'
            WHEN participant.proband_relationship =  'Paternal Aunt' THEN 'PAUNT'
            WHEN participant.proband_relationship =  'Maternal Uncle' THEN 'MUNCLE'
            WHEN participant.proband_relationship =  'Paternal Uncle' THEN 'PUNCLE'
            WHEN participant.proband_relationship =  'Niece' THEN 'NIECE'
            WHEN participant.proband_relationship =  'Nephew' THEN 'NEPHEW'
            WHEN participant.proband_relationship =  'Maternal 1st Cousin' THEN 'MCOUSN'
            WHEN participant.proband_relationship =  'Paternal 1st Cousin' THEN 'PCOUSN'
            ELSE participant.proband_relationship
        END::text as "relationship_code",
    --    GEN_UNKNOWN.has_access_policy::text as "has_access_policy",
        {{ generate_global_id(prefix='fr',descriptor=['participant.AnVIL_GREGoR_GSS_U07_GRU_participant_id'], study_id='gregor_synthetic') }}::text as "id",
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        -- join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype using (ftd_key)
    )

    select 
        * 
    from source
    