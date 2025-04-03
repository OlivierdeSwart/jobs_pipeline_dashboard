import os
import json
import snowflake.connector
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load env variables from .env
load_dotenv()

# Snowflake connection config
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": "COMPUTE_WH",
    "database": "JOBS",
    "schema": "STA"
}

def connect_snowflake():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def load_json_to_variant(json_path, source_date):
    conn = connect_snowflake()
    cs = conn.cursor()

    try:
        cs.execute("""
            CREATE TABLE IF NOT EXISTS jobs.sta.jobs_raw (
                id INTEGER AUTOINCREMENT PRIMARY KEY,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_date DATE,
                raw_data VARIANT
            )
        """)

        with open(json_path) as f:
            jobs = json.load(f)

        insert_sql = """
            INSERT INTO jobs.sta.jobs_raw (source_date, raw_data)
            SELECT %s, PARSE_JSON(%s)
        """

        for job in jobs:
            cs.execute(insert_sql, (source_date, json.dumps(job)))

        print(f"✅ Inserted {len(jobs)} jobs for {source_date}")

    except Exception as e:
        print("❌ Error loading data:", e)

    finally:
        cs.close()
        conn.close()

if __name__ == "__main__":
    folder = "data_storage/remotive_job_api/2025-04-01"
    json_path = os.path.join(folder, "daily.json")
    source_date = Path(folder).name  # "2025-04-01"
    load_json_to_variant(json_path, source_date)
