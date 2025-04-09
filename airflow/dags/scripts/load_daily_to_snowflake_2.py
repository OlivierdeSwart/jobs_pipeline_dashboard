import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from snowflake import connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path="/opt/airflow/.env")

print("👀 SF User:", os.getenv("SNOWFLAKE_USER"))

# Connect to Snowflake
ctx = connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
)

print('✅ Connected to Snowflake')

def load_json_to_variant(json_path):
    cs = ctx.cursor()

    try:
        # Create or replace the table (refreshes daily)
        cs.execute("""
            CREATE OR REPLACE TABLE JOBS.STA.JOBS_RAW (
                RAW_DATA VARIANT,
                META_INGESTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                META_SOURCE_NAME STRING DEFAULT 'REMOTIVE_JOB_API'
            )
        """)

        # Load JSON
        with open(json_path) as f:
            jobs = json.load(f)
            print(f"✅ Loaded {len(jobs)} jobs from JSON")

        insert_sql = """
            INSERT INTO JOBS.STA.JOBS_RAW (RAW_DATA)
            SELECT PARSE_JSON(%s)
        """

        for job in jobs:
            cs.execute(insert_sql, (json.dumps(job),))

        print(f"✅ Inserted {len(jobs)} jobs into JOBS.STA.JOBS_RAW")

    except Exception as e:
        print("❌ Error loading data:", e)

    finally:
        cs.close()
        ctx.close()
        print("🔒 Closed connection")


if __name__ == "__main__":
    two_days_ago = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
    folder = f"/opt/airflow/data_storage/remotive_job_api/{two_days_ago}"
    json_path = os.path.join(folder, "daily.json")
    load_json_to_variant(json_path)