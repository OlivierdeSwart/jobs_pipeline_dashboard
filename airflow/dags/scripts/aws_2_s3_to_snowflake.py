import os
from datetime import datetime
from snowflake.connector import connect
from dotenv import load_dotenv

# Load env vars from MWAA or local .env
load_dotenv(dotenv_path="/opt/airflow/.env")  # Adjust or remove if using MWAA Env Vars

def load_s3_to_snowflake():
    # 🔁 Use today's date to form S3 path
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    s3_path = f"@JOBS.STA.MY_S3_STAGE/remotive_job_api/{today_str}/daily.ndjson"

    print(f"🚀 Starting COPY INTO from {s3_path}")

    # ❄️ Connect to Snowflake
    conn = connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
    )
    cursor = conn.cursor()

    try:
        # ✅ Optional truncate before load
        print("🧹 Truncating target table...")
        cursor.execute("TRUNCATE TABLE IF EXISTS JOBS.STA.JOBS_RAW")

        # ✅ Run COPY INTO
        print("📥 Loading from S3...")
        cursor.execute(f"""
            COPY INTO JOBS.STA.JOBS_RAW (RAW_DATA)
            FROM (
                SELECT $1::VARIANT FROM {s3_path}
            )
            FILE_FORMAT = (TYPE = JSON)
        """)

        print("✅ COPY INTO completed successfully.")

    finally:
        cursor.close()
        conn.close()
        print("🔒 Closed Snowflake connection")

if __name__ == "__main__":
    load_s3_to_snowflake()