import duckdb
conn = duckdb.connect('clickstream.duckdb')
conn.execute("DROP TABLE IF EXISTS session_summary")
conn.execute("CREATE TABLE session_summary AS SELECT * FROM read_parquet('./data_lake/gold/session_summary/*.parquet')")
conn.execute("COPY (SELECT * FROM session_summary) TO 'session_sample.csv' WITH (HEADER TRUE)")
print("Exported sample CSV to session_sample.csv")
