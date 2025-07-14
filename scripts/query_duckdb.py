#!/usr/bin/env python
import duckdb

con = duckdb.connect("/tmp/dbt.duckdb")

# con.execute( "COPY (SELECT * FROM main_GREGoR_R03_HMB_20250612_data.GREGoR_R03_HMB_20250612_ftd_subject) TO 'GREGoR_R03_HMB_subject.csv' (HEADER, DELIMITER ',')")
# con.execute( "COPY (SELECT * FROM main_GREGoR_R03_HMB_20250612_data.GREGoR_R03_HMB_20250612_ftd_subject_external_id) TO 'GREGoR_R03_HMB_subject_external_id.csv' (HEADER, DELIMITER ',')")
# con.execute( "COPY (SELECT * FROM main_GREGoR_R03_HMB_20250612_data.GREGoR_R03_HMB_20250612_ftd_subjectassertion) TO 'GREGoR_R03_HMB_subjectassertion.csv' (HEADER, DELIMITER ',')")
# con.execute( "COPY (SELECT * FROM main_GREGoR_R03_HMB_20250612_data.GREGoR_R03_HMB_20250612_ftd_subjectassertion_external_id) TO 'GREGoR_R03_HMB_subjectassertion__external_id.csv' (HEADER, DELIMITER ',')")
con.execute( "COPY (SELECT * FROM main_GREGoR_R03_HMB_20250612_data.GREGoR_R03_HMB_20250612_ftd_demographics) TO 'GREGoR_R03_HMB_demographics.csv' (HEADER, DELIMITER ',')")
# con.execute( "COPY (SELECT * FROM main_GREGoR_R03_HMB_20250612_data.GREGoR_R03_HMB_20250612_ftd_demographics_external_id) TO 'GREGoR_R03_HMB_demographics_external_id.csv' (HEADER, DELIMITER ',')")
# con.execute( "COPY (SELECT * FROM main_GREGoR_R03_HMB_20250612_data.GREGoR_R03_HMB_20250612_ftd_demographics_race) TO 'GREGoR_R03_HMB_demographics_race.csv' (HEADER, DELIMITER ',')")
# con.execute( "COPY (SELECT * FROM main_GREGoR_R03_HMB_20250612_data.GREGoR_R03_HMB_20250612_ftd_familyrelationship) TO 'GREGoR_R03_HMB_familyrelationship.csv' (HEADER, DELIMITER ',')")
# con.execute( "COPY (SELECT * FROM main_GREGoR_R03_HMB_20250612_data.GREGoR_R03_HMB_20250612_ftd_familyrelationship_external_id) TO 'GREGoR_R03_HMB_familyrelationship_external_id.csv' (HEADER, DELIMITER ',')")
# con.execute( "COPY (SELECT * FROM main_GREGoR_R03_HMB_20250612_data.GREGoR_R03_HMB_20250612_ftd_familymember) TO 'GREGoR_R03_HMB_familymember.csv' (HEADER, DELIMITER ',')")
# con.execute( "COPY (SELECT * FROM main_GREGoR_R03_HMB_20250612_data.GREGoR_R03_HMB_20250612_ftd_familymember_external_id) TO 'GREGoR_R03_HMB_familymember_external_id.csv' (HEADER, DELIMITER ',')")
# con.execute( "COPY (SELECT * FROM main_GREGoR_R03_HMB_20250612_data.GREGoR_R03_HMB_20250612_ftd_accesspolicy_access_policy_code) TO 'GREGoR_R03_HMB_accesspolicy_access_policy_code.csv' (HEADER, DELIMITER ',')")
# con.execute( "COPY (SELECT * FROM main_GREGoR_R03_HMB_20250612_data.GREGoR_R03_HMB_20250612_ftd_accesspolicy) TO 'GREGoR_R03_HMB_accesspolicy.csv' (HEADER, DELIMITER ',')")

# +
# tables = con.execute(
#     "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main_GREGoR_R03_GRU_20250612_data'"
# ).fetchall()
# print(tables)
# # table = con.execute("PRAGMA table_info('main_GREGoR_R03_GRU_20250612_data.GREGoR_R03_GRU_20250612_ftd_demographics')").fetchall()
# # print(table)

# +
# d = con.execute(
#     "SELECT * FROM main_GREGoR_R03_GRU_20250612_data.GREGoR_R03_GRU_20250612_ftd_demographics LIMIT 10"
# ).fetchall()
# print(f"FTD Demographics\n {d}\n")

# +
# de = con.execute(
#     "SELECT * FROM main_GREGoR_R03_GRU_20250612_data.GREGoR_R03_GRU_20250612_ftd_demographics_external_id LIMIT 10"
# ).fetchall()
# print(f"FTD Demographics External Id\n {de}\n")

# +
# dr = con.execute(
#     "SELECT * FROM main_GREGoR_R03_GRU_20250612_data.GREGoR_R03_GRU_20250612_ftd_demographics_race LIMIT 10"
# ).fetchall()
# print(f"FTD Demographics Race\n {dr}\n")


# +
# fr = con.execute(
#     "SELECT * FROM main_GREGoR_R03_GRU_20250612_data.GREGoR_R03_GRU_20250612_ftd_familyrelationship LIMIT 10"
# ).fetchall()
# print(f"FTD Family Relationship\n {fr}\n")

# +
# fre = con.execute(
#     "SELECT * FROM main_GREGoR_R03_GRU_20250612_data.GREGoR_R03_GRU_20250612_ftd_familyrelationship_external_id LIMIT 10"
# ).fetchall()
# print(f"FTD Family Relationship External Id\n {fre}\n")

# +
# fm = con.execute(
#     "SELECT * FROM main_GREGoR_R03_GRU_20250612_data.GREGoR_R03_GRU_20250612_ftd_familymember LIMIT 10"
# ).fetchall()
# print(f"FTD Family Member\n {fm}\n")

# +
# fme = con.execute(
#     "SELECT * FROM main_GREGoR_R03_GRU_20250612_data.GREGoR_R03_GRU_20250612_ftd_familymember_external_id LIMIT 10"
# ).fetchall()
# print(f"FTD Family Member External Id\n {fme}\n")

# +
# s = con.execute(
#     "SELECT * FROM main_GREGoR_R03_GRU_20250612_data.GREGoR_R03_GRU_20250612_ftd_subject LIMIT 10"
# ).fetchall()
# print(f"FTD Subject\n {s}\n")

# +
# se = con.execute(
#     "SELECT * FROM main_GREGoR_R03_GRU_20250612_data.GREGoR_R03_GRU_20250612_ftd_subject_external_id LIMIT 10"
# ).fetchall()
# print(f"FTD Subject External Id\n {se}\n")

# +
# sa = con.execute(
#     "SELECT * FROM main_GREGoR_R03_GRU_20250612_data.GREGoR_R03_GRU_20250612_ftd_subjectassertion LIMIT 10"
# ).fetchall()
# print(f"FTD Subject Assertion\n {sa}\n")

# +
# sae = con.execute(
#     "SELECT * FROM main_GREGoR_R03_GRU_20250612_data.GREGoR_R03_GRU_20250612_ftd_subjectassertion_external_id LIMIT 10"
# ).fetchall()
# print(f"FTD Subject Assertion External Id\n {sae}\n")

# +
# apapc = con.execute(
#     "SELECT * FROM main_GREGoR_R03_GRU_20250612_data.GREGoR_R03_GRU_20250612_ftd_accesspolicy_access_policy_code LIMIT 10"
# ).fetchall()
# print(f"FTD Access Policy Access Policy Code\n {apapc}\n")

# +
# ap = con.execute(
#     "SELECT * FROM main_GREGoR_R03_GRU_20250612_data.GREGoR_R03_GRU_20250612_ftd_accesspolicy LIMIT 10"
# ).fetchall()
# print(f"FTD Access Policy\n {ap}\n")
