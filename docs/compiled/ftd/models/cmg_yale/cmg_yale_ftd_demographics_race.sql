

select 
  coalesce(race_code,'unknown') AS "race",
  coalesce(ancestry, 'FTD_NULL') as "ftd_race", -- flag nulls for analysis
  coalesce(race_code, 'Needs Handling') as "ftd_flag_race", -- flag unhandled strings
  'dm' || '_' || md5('cmg_yale' || '|' || cast(coalesce(subject_id, '') as text))::text as "demographics_id"
from (select 
        distinct 
        ancestry,
        subject_id,
        race.code as "race_code",
        race.display as "race_display"
     from "dbt"."main_main"."cmg_yale_stg_subject"
     left join "dbt"."main"."dm_race" as race
     on lower(ancestry) = src_format      
     ) as s