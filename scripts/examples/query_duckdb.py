import duckdb

con = duckdb.connect("/tmp/dbt.duckdb")


tables = con.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main_gregor_ts_data'"
).fetchall()
print(tables)

table = con.execute(
    "PRAGMA table_info('main_gregor_ts_data.gregor_ts_ftd_participant')"
).fetchall()
print(table)
