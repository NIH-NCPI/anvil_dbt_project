

WITH split_race AS (
  SELECT
    subject_id,
    src_race
  FROM "dbt"."main_main"."cser_stg_subject",
  UNNEST(SPLIT(race_ethnicity, '|')) AS t(src_race)
),

race_alone AS (
  SELECT
    subject_id,
    CASE
      WHEN src_race IN ('Hispanic or Latino', 'Hispanic/Latino')
        THEN NULL                              
      ELSE src_race
    END AS race_val
  FROM split_race
)

select distinct
  'dm' || '_' || md5('cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "demographics_id",
  COALESCE(dr.code, 'unknown')::text as "race"
from "dbt"."main_main"."cser_stg_subject" as s
left join race_alone as ra
    using(subject_id) 
left join "dbt"."main"."dm_race" as dr
    on lower(ra.race_val) = lower(dr.src_format)