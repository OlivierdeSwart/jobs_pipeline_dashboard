import os
import json
from datetime import datetime
from snowflake import connector
from snowflake.connector import DictCursor
from dotenv import load_dotenv

# Load env
load_dotenv(dotenv_path="/opt/airflow/.env")

# Connect
ctx = connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
)
cs = ctx.cursor()

today_str = datetime.utcnow().strftime("%Y-%m-%d")
local_folder = f"/opt/airflow/data_storage/remotive_job_api/{today_str}"
json_path = os.path.join(local_folder, "daily.json")
ndjson_path = os.path.join(local_folder, "daily.ndjson")

print(f"📁 Converting {json_path} to {ndjson_path}")

# ✅ Step 1: Convert to newline-delimited JSON (NDJSON)
with open(json_path, "r") as f_in, open(ndjson_path, "w") as f_out:
    for job in json.load(f_in):
        f_out.write(json.dumps(job) + "\n")

# ✅ Step 2: Create staging area (one-time)
cs.execute("CREATE OR REPLACE TEMP STAGE jobs_json_stage")

# ✅ Step 3: Upload file to Snowflake stage
print("🚀 Uploading to Snowflake stage...")
cs.execute(f"PUT file://{ndjson_path} @jobs_json_stage OVERWRITE = TRUE")

# ✅ Step 4: Truncate table (fresh daily load)
cs.execute("TRUNCATE TABLE IF EXISTS JOBS.STA.JOBS_RAW")

# ✅ Step 5: Load into table
cs.execute("""
    COPY INTO JOBS.STA.JOBS_RAW (RAW_DATA)
    FROM (
        SELECT $1::VARIANT FROM @jobs_json_stage
    )
    FILE_FORMAT = (TYPE = JSON)
""")

print("✅ Loaded data into JOBS.STA.JOBS_RAW")

# ✅ Done
cs.close()
ctx.close()
print("🔒 Closed Snowflake connection")
