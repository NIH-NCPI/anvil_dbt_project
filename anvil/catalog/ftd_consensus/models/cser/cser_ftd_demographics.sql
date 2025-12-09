{{ config(materialized='table', schema='cser_data') }}

WITH split_race AS (
  SELECT
    subject_id,
    src_race
  FROM {{ ref('cser_stg_subject') }},
  UNNEST(SPLIT(race_ethnicity, '|')) AS t(src_race)
),

race_eth AS (
  SELECT
    subject_id,
    CASE
      WHEN src_race IN ('Hispanic or Latino', 'Hispanic/Latino')
        THEN NULL                               -- this is an ethnicity, not a race
      ELSE src_race
    END AS race_val,
    CASE
      WHEN src_race IN ('Hispanic or Latino', 'Hispanic/Latino')
        THEN 'hispanic_or_latino'
      ELSE NULL                                -- this is a race, not an ethnicity
    END AS eth_val
  FROM split_race
),

ethnicity_by_id AS (
  -- returns one row per subject with Hispanic/Latino ethnicity
  SELECT
    subject_id,
    ANY_VALUE(eth_val) AS ethnicity
  FROM race_eth
  WHERE eth_val IS NOT NULL
  GROUP BY subject_id
),

race_rows AS (
  -- rows for subjects with race
  SELECT
    re.subject_id,
    re.race_val AS race,
    COALESCE(e.ethnicity, 'unknown') AS ethnicity
  FROM race_eth re
  LEFT JOIN ethnicity_by_id e
    ON re.subject_id = e.subject_id
  WHERE re.race_val IS NOT NULL
),

unknown_race_rows AS (
  -- synthesize "race = unknown" for subjects with ethnicity but no race
  SELECT
    e.subject_id,
    'unknown' AS race,
    e.ethnicity
  FROM ethnicity_by_id e
  LEFT JOIN race_rows r
    ON e.subject_id = r.subject_id
  WHERE r.subject_id IS NULL
),

final_race_ethnicity AS (
  SELECT *
  FROM race_rows
  UNION ALL
  SELECT *
  FROM unknown_race_rows
  ORDER BY subject_id, race
)

SELECT *
FROM final_race_ethnicity

    
-- select 
-- NULL::integer as "date_of_birth",
-- NULL::text as "date_of_birth_type",
-- ds.code::text as "sex",
-- ds.display::text as "sex_display",
-- COALESCE(dr.display, 'unknown')::text as "race_display",
-- CASE 
--     WHEN sr.src_race = 'hispanic or latino' THEN 'hispanic_or_latino'
--     WHEN sr.src_race = 'hispanic/latino' THEN 'hispanic_or_latino'
--     ELSE 'unknown'
-- END::text as "ethnicity",
-- CASE 
--     WHEN sr.src_race = 'hispanic or latino' THEN 'Hispanic or Latino'
--     WHEN sr.src_race = 'hispanic/latino' THEN 'Hispanic or Latino'
--     ELSE 'unknown'
-- END::text as "ethnicity_display",
-- NULL::integer as "age_at_last_vital_status",
-- NULL::text as "vital_status",
-- subject_id,
-- {{ generate_global_id(prefix='ap',descriptor=['s.consent_id'], study_id='cser') }}::text as "has_access_policy",
-- {{ generate_global_id(prefix='dm',descriptor=['subject_id', 'consent_id'], study_id='cser') }}::text as "id"
-- from {{ ref('cser_stg_subject') }} as s
-- left join split_race as sr
--     using(subject_id) 
-- left join {{ ref('dm_race') }} as dr
--     on lower(sr.src_race) = lower(dr.src_format)
-- left join {{ ref('dm_sex') }} as ds
--     on lower(s.sex) = lower(ds.src_format)