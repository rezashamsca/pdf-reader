import duckdb
import os
import requests
import json

# connect to DuckDB in-memory
con = duckdb.connect()

# load all CSV files from the data folder
data_folder = "./data"
tables = []

for filename in os.listdir(data_folder):
    if filename.endswith(".csv"):
        table_name = filename.replace(".csv", "").replace("-", "_").replace(" ", "_")
        filepath = os.path.join(data_folder, filename)

        con.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{filepath}', header=True)
        """)

        tables.append(table_name)
        print(f"Loaded: {filename} -> table: {table_name}")

# build schema context for Ollama
schema_context = ""

for table in tables:
    schema = con.execute(f"DESCRIBE {table}").fetchall()
    sample = con.execute(f"SELECT * FROM {table} LIMIT 3").fetchall()
    schema_context += f"\nTable: {table}\nSchema: {schema}\nSample rows: {sample}\n"

print("\nAll tables loaded:")
print(schema_context)
