{{ config(materialized='table') }}

with source as (
    
    SELECT
        "alignment_method"::TEXT AS "alignment_method"
        ,
        "analyte_type"::TEXT AS "analyte_type"
        ,
        "capture_region_bed_file"::TEXT AS "capture_region_bed_file"
        ,
        "data_processing_pipeline"::TEXT AS "data_processing_pipeline"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "date_data_generation"::TEXT AS "date_data_generation"
        ,
        "exome_capture_platform"::TEXT AS "exome_capture_platform"
        ,
        NULL AS "file_id",
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "library_prep_kit_method"::TEXT AS "library_prep_kit_method"
        ,
        "reference_genome_build"::TEXT AS "reference_genome_build"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        "seq_filename"::TEXT AS "seq_filename"
        ,
        "sequencing_assay"::TEXT AS "sequencing_assay"
        ,
        "sequencing_id"::TEXT AS "sequencing_id"
        ,
        NULL AS "sequencing_id_fileref"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sequencing_ANVIL_CMG_UWash_DS_CHDEF_20250224_ANV5_202502241753_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id':'VARCHAR'
        ,'sequencing_id': 'VARCHAR'
        ,'alignment_method': 'VARCHAR'
        ,'analyte_type': 'VARCHAR'
        ,'data_processing_pipeline': 'VARCHAR'
        ,'date_data_generation': 'VARCHAR'
        ,'exome_capture_platform': 'VARCHAR'
        ,'library_prep_kit_method': 'VARCHAR'
        ,'reference_genome_build': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'seq_filename': 'VARCHAR'
        ,'sequencing_assay': 'VARCHAR'
        ,'capture_region_bed_file': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "alignment_method"::TEXT AS "alignment_method"
        ,
        "analyte_type"::TEXT AS "analyte_type"
        ,
        "capture_region_bed_file"::TEXT AS "capture_region_bed_file"
        ,
        "data_processing_pipeline"::TEXT AS "data_processing_pipeline"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "date_data_generation"::TEXT AS "date_data_generation"
        ,
        "exome_capture_platform"::TEXT AS "exome_capture_platform"
        ,
        NULL AS "file_id",
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "library_prep_kit_method"::TEXT AS "library_prep_kit_method"
        ,
        "reference_genome_build"::TEXT AS "reference_genome_build"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        "seq_filename"::TEXT AS "seq_filename"
        ,
        "sequencing_assay"::TEXT AS "sequencing_assay"
        ,
        "sequencing_id"::TEXT AS "sequencing_id"
        ,
        NULL AS "sequencing_id_fileref"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sequencing_ANVIL_CMG_UWash_GRU_IRB_20250224_ANV5_202502241723_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sequencing_id': 'VARCHAR'
        ,'alignment_method': 'VARCHAR'
        ,'analyte_type': 'VARCHAR'
        ,'capture_region_bed_file': 'VARCHAR'
        ,'data_processing_pipeline': 'VARCHAR'
        ,'date_data_generation': 'VARCHAR'
        ,'exome_capture_platform': 'VARCHAR'
        ,'library_prep_kit_method': 'VARCHAR'
        ,'reference_genome_build': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'seq_filename': 'VARCHAR'
        ,'sequencing_assay': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "alignment_method"::TEXT AS "alignment_method"
        ,
        "analyte_type"::TEXT AS "analyte_type"
        ,
        "capture_region_bed_file"::TEXT AS "capture_region_bed_file"
        ,
        "data_processing_pipeline"::TEXT AS "data_processing_pipeline"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "date_data_generation"::TEXT AS "date_data_generation"
        ,
        "exome_capture_platform"::TEXT AS "exome_capture_platform"
        ,
        NULL AS "file_id",
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "library_prep_kit_method"::TEXT AS "library_prep_kit_method"
        ,
        "reference_genome_build"::TEXT AS "reference_genome_build"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        "seq_filename"::TEXT AS "seq_filename"
        ,
        "sequencing_assay"::TEXT AS "sequencing_assay"
        ,
        "sequencing_id"::TEXT AS "sequencing_id"
        ,
        NULL AS "sequencing_id_fileref"FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sequencing_ANVIL_CMG_UWash_GRU_20250224_ANV5_202502241706_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sequencing_id': 'VARCHAR'
        ,'alignment_method': 'VARCHAR'
        ,'analyte_type': 'VARCHAR'
        ,'data_processing_pipeline': 'VARCHAR'
        ,'date_data_generation': 'VARCHAR'
        ,'exome_capture_platform': 'VARCHAR'
        ,'library_prep_kit_method': 'VARCHAR'
        ,'reference_genome_build': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'seq_filename': 'VARCHAR'
        ,'sequencing_assay': 'VARCHAR'
        ,'capture_region_bed_file': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "alignment_method"::TEXT AS "alignment_method"
        ,
        "analyte_type"::TEXT AS "analyte_type"
        ,
        NULL AS "capture_region_bed_file",
        "data_processing_pipeline"::TEXT AS "data_processing_pipeline"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "date_data_generation"::TEXT AS "date_data_generation"
        ,
        "exome_capture_platform"::TEXT AS "exome_capture_platform"
        ,
        NULL AS "file_id",
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "library_prep_kit_method"::TEXT AS "library_prep_kit_method"
        ,
        "reference_genome_build"::TEXT AS "reference_genome_build"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        NULL AS "seq_filename",
        "sequencing_assay"::TEXT AS "sequencing_assay"
        ,
        "sequencing_id"::TEXT AS "sequencing_id"
        ,
        "sequencing_id_fileref"::TEXT AS "sequencing_id_fileref"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sequencing_ANVIL_CMG_UWASH_HMB_IRB_20250219_ANV5_202502201921_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sequencing_id': 'VARCHAR'
        ,'alignment_method': 'VARCHAR'
        ,'analyte_type': 'VARCHAR'
        ,'data_processing_pipeline': 'VARCHAR'
        ,'date_data_generation': 'VARCHAR'
        ,'exome_capture_platform': 'VARCHAR'
        ,'library_prep_kit_method': 'VARCHAR'
        ,'reference_genome_build': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sequencing_assay': 'VARCHAR'
        ,'sequencing_id_fileref': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "alignment_method"::TEXT AS "alignment_method"
        ,
        "analyte_type"::TEXT AS "analyte_type"
        ,
        "capture_region_bed_file"::TEXT AS "capture_region_bed_file"
        ,
        "data_processing_pipeline"::TEXT AS "data_processing_pipeline"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "date_data_generation"::TEXT AS "date_data_generation"
        ,
        "exome_capture_platform"::TEXT AS "exome_capture_platform"
        ,
        NULL AS "file_id",
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "library_prep_kit_method"::TEXT AS "library_prep_kit_method"
        ,
        "reference_genome_build"::TEXT AS "reference_genome_build"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        "seq_filename"::TEXT AS "seq_filename"
        ,
        "sequencing_assay"::TEXT AS "sequencing_assay"
        ,
        "sequencing_id"::TEXT AS "sequencing_id"
        ,
        "sequencing_id_fileref"::TEXT AS "sequencing_id_fileref"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sequencing_ANVIL_CMG_UWASH_HMB_20250219_ANV5_202502201916_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sequencing_id': 'VARCHAR'
        ,'alignment_method': 'VARCHAR'
        ,'analyte_type': 'VARCHAR'
        ,'data_processing_pipeline': 'VARCHAR'
        ,'date_data_generation': 'VARCHAR'
        ,'exome_capture_platform': 'VARCHAR'
        ,'library_prep_kit_method': 'VARCHAR'
        ,'reference_genome_build': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'seq_filename': 'VARCHAR'
        ,'sequencing_assay': 'VARCHAR'
        ,'sequencing_id_fileref': 'VARCHAR'
        ,'capture_region_bed_file': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "alignment_method"::TEXT AS "alignment_method"
        ,
        "analyte_type"::TEXT AS "analyte_type"
        ,
        NULL AS "capture_region_bed_file",
        "data_processing_pipeline"::TEXT AS "data_processing_pipeline"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "date_data_generation"::TEXT AS "date_data_generation"
        ,
        "exome_capture_platform"::TEXT AS "exome_capture_platform"
        ,
        NULL AS "file_id",
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "library_prep_kit_method"::TEXT AS "library_prep_kit_method"
        ,
        "reference_genome_build"::TEXT AS "reference_genome_build"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        NULL AS "seq_filename",
        "sequencing_assay"::TEXT AS "sequencing_assay"
        ,
        "sequencing_id"::TEXT AS "sequencing_id"
        ,
        "sequencing_id_fileref"::TEXT AS "sequencing_id_fileref"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sequencing_ANVIL_CMG_UWASH_DS_NBIA_20250206_ANV5_202502201903_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sequencing_id': 'VARCHAR'
        ,'alignment_method': 'VARCHAR'
        ,'analyte_type': 'VARCHAR'
        ,'data_processing_pipeline': 'VARCHAR'
        ,'date_data_generation': 'VARCHAR'
        ,'exome_capture_platform': 'VARCHAR'
        ,'library_prep_kit_method': 'VARCHAR'
        ,'reference_genome_build': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sequencing_assay': 'VARCHAR'
        ,'sequencing_id_fileref': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "alignment_method"::TEXT AS "alignment_method"
        ,
        "analyte_type"::TEXT AS "analyte_type"
        ,
        NULL AS "capture_region_bed_file",
        "data_processing_pipeline"::TEXT AS "data_processing_pipeline"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "date_data_generation"::TEXT AS "date_data_generation"
        ,
        "exome_capture_platform"::TEXT AS "exome_capture_platform"
        ,
        NULL AS "file_id",
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "library_prep_kit_method"::TEXT AS "library_prep_kit_method"
        ,
        "reference_genome_build"::TEXT AS "reference_genome_build"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        NULL AS "seq_filename",
        "sequencing_assay"::TEXT AS "sequencing_assay"
        ,
        "sequencing_id"::TEXT AS "sequencing_id"
        ,
        "sequencing_id_fileref"::TEXT AS "sequencing_id_fileref"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sequencing_ANVIL_CMG_UWASH_DS_HFA_20250206_ANV5_202502201859_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sequencing_id': 'VARCHAR'
        ,'alignment_method': 'VARCHAR'
        ,'analyte_type': 'VARCHAR'
        ,'data_processing_pipeline': 'VARCHAR'
        ,'date_data_generation': 'VARCHAR'
        ,'exome_capture_platform': 'VARCHAR'
        ,'library_prep_kit_method': 'VARCHAR'
        ,'reference_genome_build': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sequencing_assay': 'VARCHAR'
        ,'sequencing_id_fileref': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "alignment_method"::TEXT AS "alignment_method"
        ,
        "analyte_type"::TEXT AS "analyte_type"
        ,
        "capture_region_bed_file"::TEXT AS "capture_region_bed_file"
        ,
        "data_processing_pipeline"::TEXT AS "data_processing_pipeline"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "date_data_generation"::TEXT AS "date_data_generation"
        ,
        "exome_capture_platform"::TEXT AS "exome_capture_platform"
        ,
        NULL AS "file_id",
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "library_prep_kit_method"::TEXT AS "library_prep_kit_method"
        ,
        "reference_genome_build"::TEXT AS "reference_genome_build"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        NULL AS "seq_filename",
        "sequencing_assay"::TEXT AS "sequencing_assay"
        ,
        "sequencing_id"::TEXT AS "sequencing_id"
        ,
        "sequencing_id_fileref"::TEXT AS "sequencing_id_fileref"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sequencing_ANVIL_CMG_UWash_DS_EP_20250219_ANV5_202502201854_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sequencing_id': 'VARCHAR'
        ,'alignment_method': 'VARCHAR'
        ,'analyte_type': 'VARCHAR'
        ,'data_processing_pipeline': 'VARCHAR'
        ,'date_data_generation': 'VARCHAR'
        ,'exome_capture_platform': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        ,'library_prep_kit_method': 'VARCHAR'
        ,'reference_genome_build': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sequencing_assay': 'VARCHAR'
        ,'sequencing_id_fileref': 'VARCHAR'
        ,'capture_region_bed_file': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "alignment_method"::TEXT AS "alignment_method"
        ,
        "analyte_type"::TEXT AS "analyte_type"
        ,
        "capture_region_bed_file"::TEXT AS "capture_region_bed_file"
        ,
        "data_processing_pipeline"::TEXT AS "data_processing_pipeline"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "date_data_generation"::TEXT AS "date_data_generation"
        ,
        "exome_capture_platform"::TEXT AS "exome_capture_platform"
        ,
        "file_id"::TEXT AS "file_id"
        ,
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "library_prep_kit_method"::TEXT AS "library_prep_kit_method"
        ,
        "reference_genome_build"::TEXT AS "reference_genome_build"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        NULL AS "seq_filename",
        "sequencing_assay"::TEXT AS "sequencing_assay"
        ,
        "sequencing_id"::TEXT AS "sequencing_id"
        ,
        "sequencing_id_fileref"::TEXT AS "sequencing_id_fileref"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sequencing_ANVIL_CMG_UWASH_DS_BDIS_20250206_ANV5_202502201850_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sequencing_id': 'VARCHAR'
        ,'alignment_method': 'VARCHAR'
        ,'analyte_type': 'VARCHAR'
        ,'data_processing_pipeline': 'VARCHAR'
        ,'date_data_generation': 'VARCHAR'
        ,'exome_capture_platform': 'VARCHAR'
        ,'file_id': 'VARCHAR'
        ,'library_prep_kit_method': 'VARCHAR'
        ,'reference_genome_build': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sequencing_assay': 'VARCHAR'
        ,'sequencing_id_fileref': 'VARCHAR'
        ,'capture_region_bed_file': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
    UNION ALL
    SELECT
        "alignment_method"::TEXT AS "alignment_method"
        ,
        "analyte_type"::TEXT AS "analyte_type"
        ,
        "capture_region_bed_file"::TEXT AS "capture_region_bed_file"
        ,
        "data_processing_pipeline"::TEXT AS "data_processing_pipeline"
        ,
        "datarepo_row_id"::TEXT AS "datarepo_row_id"
        ,
        "date_data_generation"::TEXT AS "date_data_generation"
        ,
        "exome_capture_platform"::TEXT AS "exome_capture_platform"
        ,
        NULL AS "file_id",
        "ingest_provenance"::TEXT AS "ingest_provenance"
        ,
        "library_prep_kit_method"::TEXT AS "library_prep_kit_method"
        ,
        "reference_genome_build"::TEXT AS "reference_genome_build"
        ,
        "sample_id"::TEXT AS "sample_id"
        ,
        NULL AS "seq_filename",
        "sequencing_assay"::TEXT AS "sequencing_assay"
        ,
        "sequencing_id"::TEXT AS "sequencing_id"
        ,
        "sequencing_id_fileref"::TEXT AS "sequencing_id_fileref"
        FROM read_csv('/home/jupyter/pipeline/anvil_dbt_project/data/cmg_uwash/sequencing_ANVIL_CMG_UWASH_DS_BAV_IRB_PUB_RD_20250206_ANV5_202502201846_000000000000.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={'datarepo_row_id': 'VARCHAR'
        ,'sequencing_id': 'VARCHAR'
        ,'alignment_method': 'VARCHAR'
        ,'analyte_type': 'VARCHAR'
        ,'capture_region_bed_file': 'VARCHAR'
        ,'data_processing_pipeline': 'VARCHAR'
        ,'date_data_generation': 'VARCHAR'
        ,'exome_capture_platform': 'VARCHAR'
        ,'library_prep_kit_method': 'VARCHAR'
        ,'reference_genome_build': 'VARCHAR'
        ,'sample_id': 'VARCHAR'
        ,'sequencing_assay': 'VARCHAR'
        ,'sequencing_id_fileref': 'VARCHAR'
        ,'ingest_provenance': 'VARCHAR'
        })
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source
