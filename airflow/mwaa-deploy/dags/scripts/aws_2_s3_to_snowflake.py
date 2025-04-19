import boto3
import json
from datetime import datetime
from snowflake.connector import connect

def get_snowflake_secrets():
    secret_name = "one-secret-to-rule-them-all"
    region_name = "us-east-1"

    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])

    return secret["snowflake"]

def load_s3_to_snowflake():
    # 🔁 Use today's date to form S3 path
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    s3_path = f"@JOBS.STA.MY_S3_STAGE/remotive_job_api/{today_str}/daily.ndjson"
    print(f"🚀 Starting COPY INTO from {s3_path}")

    # 🔐 Load Snowflake secrets
    creds = get_snowflake_secrets()

    # ❄️ Connect to Snowflake
    conn = connect(
        user=creds["user"],
        password=creds["password"],
        account=creds["account"],
        warehouse=creds["warehouse"],
        database=creds["database"],
        schema=creds["schema"],
    )
    cursor = conn.cursor()

    try:
        print("🧹 Truncating target table...")
        cursor.execute("TRUNCATE TABLE IF EXISTS JOBS.STA.JOBS_RAW")

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
    