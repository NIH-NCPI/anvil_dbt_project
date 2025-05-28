#!/usr/bin/env python
import duckdb

con = duckdb.connect("/tmp/dbt.duckdb")

tables = con.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main_gregor_synthetic_data'"
).fetchall()
print(tables)
# table = con.execute("PRAGMA table_info('main_gregor_synthetic_data.gregor_synthetic_ftd_demographics')").fetchall()
# print(table)

d = con.execute(
    "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_demographics LIMIT 10"
).fetchall()
print(f"FTD Demographics\n {d}\n")

de = con.execute(
    "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_demographics_external_id LIMIT 10"
).fetchall()
print(f"FTD Demographics\n {de}\n")

dr = con.execute(
    "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_demographics_race LIMIT 10"
).fetchall()
print(f"FTD Demographics\n {dr}\n")

fm = con.execute(
    "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_familymember LIMIT 10"
).fetchall()
print(f"FTD Demographics\n {fm}\n")

fme = con.execute(
    "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_familymember_external_id LIMIT 10"
).fetchall()
print(f"FTD Demographics\n {fme}\n")

fr = con.execute(
    "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_familyrelationship LIMIT 10"
).fetchall()
print(f"FTD Demographics\n {fr}\n")

fre = con.execute(
    "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_familyrelationship_external_id LIMIT 10"
).fetchall()
print(f"FTD Demographics\n {fre}\n")

# ap = con.execute(
#     "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_accesspolicy LIMIT 10"
# ).fetchall()
# print(f"FTD AccessPolicy\n {ap}\n")

s = con.execute(
    "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_subject LIMIT 10"
).fetchall()
print(f"FTD Subject\n {s}\n")

se = con.execute(
    "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_subject_external_id LIMIT 10"
).fetchall()
print(f"FTD Subject External Id\n {se}\n")

sa = con.execute(
    "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_subjectassertion LIMIT 10"
).fetchall()
print(f"FTD Subject\n {sa}\n")

sae = con.execute(
    "aLECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_subjectassertion_external_id LIMIT 10"
).fetchall()
print(f"FTD Subject External Id\n {sae}\n")

apapc = con.execute(
    "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_accesspolicy_access_policy_code LIMIT 10"
).fetchall()
print(f"FTD Access Policy Access Policy Code\n {apapc}\n")
