

with source as (
    select 
      "sequencing_id"::text as "sequencing_id",
       "alignment_method"::text as "alignment_method",
       "analyte_type"::text as "analyte_type",
       "capture_region_bed_file"::text as "capture_region_bed_file",
       "data_processing_pipeline"::text as "data_processing_pipeline",
       "date_data_generation"::text as "date_data_generation",
       "exome_capture_platform"::text as "exome_capture_platform",
       "functional_equivalence_standard"::text as "functional_equivalence_standard",
       "index_file"::text as "index_file",
       "library_prep_kit_method"::text as "library_prep_kit_method",
       "number_of_independent_libraries"::text as "number_of_independent_libraries",
       "read_length"::text as "read_length",
       "reference_genome_build"::text as "reference_genome_build",
       "sample_id"::text as "sample_id",
       "seq_filename"::text as "seq_filename",
       "ingest_provenance"::text as "ingest_provenance"
    from "dbt"."main"."cser_sequencing"
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source