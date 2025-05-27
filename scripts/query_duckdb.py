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

# ap = con.execute(
#     "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_accesspolicy LIMIT 10"
# ).fetchall()
# print(f"FTD AccessPolicy\n {ap}\n")

# s = con.execute(
#     "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_subject LIMIT 10"
# ).fetchall()
# print(f"FTD Subject\n {s}\n")

apapc = con.execute(
    "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_accesspolicy_access_policy_code LIMIT 10"
).fetchall()
print(f"FTD Access Policy Access Policy Code\n {apapc}\n")
