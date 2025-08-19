{{ config(materialized='table') }}

with source as (
    select 
      "subject_id"::text as "subject_id",
       "sex"::text as "sex",
       "decade_birth"::text as "decade_birth",
       "year_birth"::text as "year_birth",
       "ethnicity"::text as "ethnicity",
       "race"::text as "race",
       "case_control_aaa"::text as "case_control_aaa",
       "case_control_acei"::text as "case_control_acei",
       "case_control_adhd"::text as "case_control_adhd",
       "case_control_amd"::text as "case_control_amd",
       "case_control_appendicitis"::text as "case_control_appendicitis",
       "case_control_asthma"::text as "case_control_asthma",
       "case_control_atopicdermatitis"::text as "case_control_atopicdermatitis",
       "case_control_autism"::text as "case_control_autism",
       "case_control_bph"::text as "case_control_bph",
       "case_control_caad"::text as "case_control_caad",
       "case_control_camrsa"::text as "case_control_camrsa",
       "case_control_cataract"::text as "case_control_cataract",
       "case_control_cdiff"::text as "case_control_cdiff",
       "case_control_childhoodobesity"::text as "case_control_childhoodobesity",
       "case_control_ckd"::text as "case_control_ckd",
       "case_control_ckd_t2d"::text as "case_control_ckd_t2d",
       "case_control_ckd_t2d_htn"::text as "case_control_ckd_t2d_htn",
       "case_control_colonpolyps"::text as "case_control_colonpolyps",
       "case_control_crf"::text as "case_control_crf",
       "case_control_dementia"::text as "case_control_dementia",
       "case_control_div"::text as "case_control_div",
       "case_control_dr"::text as "case_control_dr",
       "case_control_extremeobesity"::text as "case_control_extremeobesity",
       "case_control_gerd"::text as "case_control_gerd",
       "case_control_glaucoma"::text as "case_control_glaucoma",
       "case_control_height"::text as "case_control_height",
       "case_control_hf"::text as "case_control_hf",
       "case_control_hypothyroidism"::text as "case_control_hypothyroidism",
       "case_control_lipids"::text as "case_control_lipids",
       "case_control_ocularhtn"::text as "case_control_ocularhtn",
       "case_control_pad"::text as "case_control_pad",
       "case_control_qrs"::text as "case_control_qrs",
       "case_control_rbc"::text as "case_control_rbc",
       "case_control_remissiondiabetes"::text as "case_control_remissiondiabetes",
       "case_control_reshyp"::text as "case_control_reshyp",
       "case_control_statinsmace"::text as "case_control_statinsmace",
       "case_control_t2d"::text as "case_control_t2d",
       "case_control_vte"::text as "case_control_vte",
       "case_control_wbc"::text as "case_control_wbc",
       "case_control_zoster"::text as "case_control_zoster"
    from {{ source('GWAS','GWAS_CaseControl_DD_20201027') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source
