#!/usr/bin/env python
from anvil_dbt_project.scripts.general.common import engine

tables = engine.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main_gregor_synthetic_data'"
).fetchall()
print(tables)

table = engine.execute(
    "PRAGMA table_info('main_gregor_synthetic_data.gregor_synthetic_ftd_demographics')"
).fetchall()
print(table)

d = engine.execute(
    "SELECT * FROM main_gregor_synthetic_data.gregor_synthetic_ftd_demographics LIMIT 10"
).fetchall()
print(f"FTD Demographics\n {d}\n")
