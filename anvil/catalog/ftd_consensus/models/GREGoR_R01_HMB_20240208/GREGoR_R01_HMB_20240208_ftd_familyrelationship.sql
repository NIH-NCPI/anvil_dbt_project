{{ config(materialized='table', schema='GREGoR_R01_HMB_20240208_data') }}

    with source as (
        select 
        
        (select DISTINCT CAST(participant_id AS STRING) FROM {{ ref('GREGoR_R01_HMB_20240208_stg_participant') }} AS stg WHERE CAST(stg.family_id AS STRING) = CAST(participant.family_id AS STRING) AND CAST(stg.proband_relationship AS STRING) = 'Self') AS other_family_member,
        participant.proband_relationship AS proband_relationship, participant.participant_id AS participant_id,
--         CASE 
--             WHEN participant.paternal_id != '0' THEN 'KIN:028'
--             WHEN participant.maternal_id != '0' THEN 'KIN:027'
--             WHEN participant.twin_id != '' AND participant.twin_id != '0' THEN 'KIN:009'
--             WHEN participant.proband_relationship =  'Mother' THEN 'MTH'
--             WHEN participant.proband_relationship =  'Father' THEN 'FTH'
--             WHEN participant.proband_relationship =  'Sibling' THEN 'SIB'
--             WHEN participant.proband_relationship =  'Child' THEN 'CHILD'
--             WHEN participant.proband_relationship =  'Maternal Half Sibling' THEN 'HSIB'
--             WHEN participant.proband_relationship =  'Paternal Half Sibling' THEN 'HSIB'
--             WHEN participant.proband_relationship =  'Maternal Grandmother' THEN 'MGRMTH'
--             WHEN participant.proband_relationship =  'Maternal Grandfather' THEN 'MGRFTH'
--             WHEN participant.proband_relationship =  'Paternal Grandmother' THEN 'PGRMTH'
--             WHEN participant.proband_relationship =  'Paternal Grandfather' THEN 'PGRFTH'
--             WHEN participant.proband_relationship =  'Maternal Aunt' THEN 'MAUNT'
--             WHEN participant.proband_relationship =  'Paternal Aunt' THEN 'PAUNT'
--             WHEN participant.proband_relationship =  'Maternal Uncle' THEN 'MUNCLE'
--             WHEN participant.proband_relationship =  'Paternal Uncle' THEN 'PUNCLE'
--             WHEN participant.proband_relationship =  'Niece' THEN 'NIECE'
--             WHEN participant.proband_relationship =  'Nephew' THEN 'NEPHEW'
--             WHEN participant.proband_relationship =  'Maternal 1st Cousin' THEN 'MCOUSN'
--             WHEN participant.proband_relationship =  'Paternal 1st Cousin' THEN 'PCOUSN'
--             ELSE participant.proband_relationship
--         END::text as "relationship_code",       
--         { { generate_global_id(prefix='ap',descriptor=['participant.consent_code'], study_id='GREGoR_R01_HMB_20240208') }}::text as "has_access_policy",
        {{ generate_global_id(prefix='sb',descriptor=['participant.participant_id'], study_id='GREGoR_R01_HMB_20240208') }}::text as "family_member"
        from {{ ref('GREGoR_R01_HMB_20240208_stg_participant') }} as participant
--         join { { ref('GREGoR_R01_HMB_20240208_stg_phenotype') }} as phenotype
-- on participant.participant_id = phenotype.participant_id 
        where participant.proband_relationship != 'Self'
    )

    select 
        {{ generate_global_id(prefix='fm',descriptor=['participant_id','source.other_family_member'],study_id='GREGoR_R01_HMB_20240208') }}::text as "id",
        proband_relationship, family_member, {{ generate_global_id(prefix='sb',descriptor=['source.other_family_member'], study_id='GREGoR_R01_HMB_20240208') }}::text as "other_family_member",
    from source
    
