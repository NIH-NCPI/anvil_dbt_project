

with source as (
    select 
      "subject_id"::text as "subject_id",
       "sex"::text as "sex",
       "decade_birth"::text as "decade_birth",
       "year_birth"::text as "year_birth",
       "ethnicity"::text as "ethnicity",
       "race"::text as "race",
       "median_weight"::text as "median_weight",
       "median_height"::text as "median_height",
       "median_bmi"::text as "median_bmi"
    from "dbt"."main"."PGRNseq_Demographics_DS_2015"
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source