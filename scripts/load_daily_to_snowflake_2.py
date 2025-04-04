import os
import json
from datetime import datetime
from pathlib import Path
from snowflake import connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Connect to Snowflake
ctx = connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    enable_connection_diag=True,
    connection_diag_log_path="<HOME>/diag-tests",
)
print('✅ Connected to Snowflake')

def load_json_to_variant(json_path, source_date):
    cs = ctx.cursor()

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
        ctx.close()
        print("🔒 Closed connection")

if __name__ == "__main__":
    folder = "data_storage/remotive_job_api/2025-04-01"
    json_path = os.path.join(folder, "daily.json")
    source_date = Path(folder).name  # e.g. "2025-04-01"
    load_json_to_variant(json_path, source_date)
    