{{ config(materialized='table') }}

with source as (
    select 
      "sequencing_id"::text as "sequencing_id",
       "alignment_method"::text as "alignment_method",
       "analyte_type"::text as "analyte_type",
       "data_processing_pipeline"::text as "data_processing_pipeline",
       "date_data_generation"::text as "date_data_generation",
       "functional_equivalence_standard"::text as "functional_equivalence_standard",
       "library_prep_kit_method"::text as "library_prep_kit_method",
       "reference_genome_build"::text as "reference_genome_build",
       "sample_id"::text as "sample_id",
       "seq_filename"::text as "seq_filename",
       "sequencing_assay"::text as "sequencing_assay",
       "ingest_provenance"::text as "ingest_provenance",
       "sequencing_id_fileref"::text as "sequencing_id_fileref",
       "capture_region_bed_file"::text as "capture_region_bed_file",
       "exome_capture_platform"::text as "exome_capture_platform"
    from {{ source('cmg_yale','sequencing') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*,
  REPLACE(ingest_provenance, 'sequencing_', '') AS "consent_id",
from source
