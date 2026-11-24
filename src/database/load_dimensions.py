import duckdb
import psycopg
import os

PARQUET_GLOB = os.environ["PARQUET_GLOB"]

print(f"[dimensions] Input: Silver Parquet -> {PARQUET_GLOB}", flush=True)
print("[dimensions] Process: select distinct state/county IDs and names; insert or update lookup tables", flush=True)

duckdb_conn = duckdb.connect()

county_dimensions_query = """
    SELECT DISTINCT state_code, county_code, county_name
    FROM read_parquet(?);
"""

state_dimensions_query = """
    SELECT DISTINCT state_code, state_name
    FROM read_parquet(?);
"""

insert_states = """
    INSERT INTO dim_state (state_code, state_name)
    VALUES (%s, %s)
    ON CONFLICT (state_code)
    DO UPDATE SET state_name = EXCLUDED.state_name
"""

insert_counties = """
    INSERT INTO dim_county (state_code, county_code, county_name)
    VALUES(%s, %s, %s)
    ON CONFLICT (state_code, county_code)
    DO UPDATE SET county_name = EXCLUDED.county_name;
"""

state_rows = duckdb_conn.execute(state_dimensions_query, [PARQUET_GLOB]).fetchall()
county_rows = duckdb_conn.execute(county_dimensions_query, [PARQUET_GLOB]).fetchall()

with psycopg.connect() as pg_conn:
    with pg_conn.cursor() as cursor:
        cursor.executemany(insert_states, state_rows)
        cursor.executemany(insert_counties, county_rows)

print(f"[dimensions] Output: PostgreSQL {os.environ['PGDATABASE']} -> dim_state ({len(state_rows):,} states), dim_county ({len(county_rows):,} counties)", flush=True)
