#!/usr/bin/env python
import duckdb
con = duckdb.connect("/tmp/dbt.duckdb")
# con.execute("COPY (SELECT * FROM main_PGRNseq_data.PGRNseq_ftd_accesspolicy) TO 'PGRNseq_accesspolicy.csv' (HEADER, DELIMITER ',')")
# con.execute("COPY (SELECT * FROM main_PGRNseq_data.PGRNseq_ftd_accesspolicy_access_policy_code) TO 'PGRNseq_access_policy_code.csv' (HEADER, DELIMITER ',')")
# con.execute("COPY (SELECT * FROM main_PGRNseq_data.PGRNseq_ftd_demographics) TO 'PGRNseq_demographics.csv' (HEADER, DELIMITER ',')")
# con.execute("COPY (SELECT * FROM main_PGRNseq_data.PGRNseq_ftd_demographics_external_id) TO 'PGRNseq_demographics_external_id.csv' (HEADER, DELIMITER ',')")
# con.execute("COPY (SELECT * FROM main_PGRNseq_data.PGRNseq_ftd_demographics_race) TO 'PGRNseq_demographics_race.csv' (HEADER, DELIMITER ',')")
# con.execute("COPY (SELECT * FROM main_PGRNseq_data.PGRNseq_ftd_sample) TO 'PGRNseq_sample.csv' (HEADER, DELIMITER ',')")
# con.execute("COPY (SELECT * FROM main_PGRNseq_data.PGRNseq_ftd_sample_external_id) TO 'PGRNseq_sample_external_id.csv' (HEADER, DELIMITER ',')")
# con.execute("COPY (SELECT * FROM main_PGRNseq_data.PGRNseq_ftd_subject) TO 'PGRNseq_subject.csv' (HEADER, DELIMITER ',')")
# con.execute("COPY (SELECT * FROM main_PGRNseq_data.PGRNseq_ftd_subject_external_id) TO 'PGRNseq_subject_external_id.csv' (HEADER, DELIMITER ',')")
# con.execute("COPY (SELECT * FROM main_PGRNseq_data.PGRNseq_ftd_subjectassertion) TO 'PGRNseq_subjectassertion.csv' (HEADER, DELIMITER ',')")


# +
# tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main_gregor_synthetic_data'").fetchall()
# print(tables)
# -

# table = con.execute("PRAGMA table_info('main_gregor_synthetic_data.gregor_synthetic_ftd_demographics')").fetchall()
# print(table)

# +
# d = con.execute("SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_demographics LIMIT 10").fetchall()
# print(f"FTD Demographics\n {d}\n")
# -

ap = con.execute("SELECT * FROM main_GWAS_data.GWAS_ftd_subjectassertion limit 20").fetchall()
print(f"FTD Subject Assertion\n {ap}\n")

# +
# s = con.execute("SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_subject LIMIT 10").fetchall()
# print(f"FTD Subject\n {s}\n")
