import duckdb
con = duckdb.connect("/tmp/dbt.duckdb")


tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
print(tables)

table = con.execute("PRAGMA table_info('main.GREGoR_synthetic_participant_revised07Aug2024')").fetchall()
print(table)