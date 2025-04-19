import json
import requests
from datetime import datetime
import boto3

def run_ingest_all():
    url = "https://remotive.com/api/remote-jobs"
    response = requests.get(url)
    jobs = response.json()["jobs"]

    # Define S3 bucket and key prefixes
    s3 = boto3.client("s3")
    bucket = "olivier-jobs-pipeline-storage"
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    prefix = f"raw/remotive_job_api/{today_str}/"

    # Pretty JSON file (for reference/debugging)
    pretty_json_key = prefix + "daily.json"
    s3.put_object(
        Bucket=bucket,
        Key=pretty_json_key,
        Body=json.dumps(jobs, indent=2),
        ContentType="application/json"
    )

    # NDJSON file (for Snowflake ingestion)
    ndjson_lines = "\n".join(json.dumps(job) for job in jobs)
    ndjson_key = prefix + "daily.ndjson"
    s3.put_object(
        Bucket=bucket,
        Key=ndjson_key,
        Body=ndjson_lines,
        ContentType="application/x-ndjson"
    )

    print(f"✅ Uploaded {len(jobs)} jobs to S3:")
    print(f"  - s3://{bucket}/{pretty_json_key}")
    print(f"  - s3://{bucket}/{ndjson_key}")

if __name__ == "__main__":
    run_ingest_all()
    