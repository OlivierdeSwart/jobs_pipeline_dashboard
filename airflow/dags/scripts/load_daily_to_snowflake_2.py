import os
import json
from datetime import datetime
from pathlib import Path
from snowflake import connector
from dotenv import load_dotenv
from datetime import datetime, timedelta

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
        # Create table (use UPPERCASE for identifiers)
        cs.execute("""
            CREATE TABLE IF NOT EXISTS JOBS.STA.JOBS_RAW (
                ID INTEGER AUTOINCREMENT PRIMARY KEY,
                INGESTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                SOURCE_DATE DATE,
                RAW_DATA VARIANT
            )
        """)

        # Load JSON
        with open(json_path) as f:
            jobs = json.load(f)
            print("✅ Loaded JSON file into local variable")
            # print("🔍 First item preview:", json.dumps(jobs[0], indent=2) if jobs else "No data loaded")

        for job in jobs[:3]:  # Just show first 3 to avoid flooding
            json_string = json.dumps(job)
            # print("🔢 Inserting:", {"source_date": source_date, "json": json_string[:80] + "..."})

        # Insert each job
        insert_sql = """
            INSERT INTO JOBS.STA.JOBS_RAW (SOURCE_DATE, RAW_DATA)
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
    two_days_ago = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
    folder = f"data_storage/remotive_job_api/{two_days_ago}"
    # folder = "data_storage/remotive_job_api/2025-04-02"
    json_path = os.path.join(folder, "daily.json")
    source_date = Path(folder).name  # "2025-04-01"
    load_json_to_variant(json_path, source_date)
    