import os
import json
from datetime import datetime
import requests

def run_ingest_all():
    url = "https://remotive.com/api/remote-jobs"
    response = requests.get(url)
    jobs = response.json()["jobs"]

    # Folder based on current UTC date
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    folder = f"/opt/airflow/data_storage/remotive_job_api/{today_str}/"
    os.makedirs(folder, exist_ok=True)

    # Save pretty JSON for debugging
    json_path = os.path.join(folder, "daily.json")
    with open(json_path, "w") as f:
        json.dump(jobs, f, indent=2)

    # Save NDJSON for Snowflake COPY INTO
    ndjson_path = os.path.join(folder, "daily.ndjson")
    with open(ndjson_path, "w") as f:
        for job in jobs:
            f.write(json.dumps(job) + "\n")

    print(f"✅ Saved {len(jobs)} jobs to:")
    print(f"  - JSON: {json_path}")
    print(f"  - NDJSON: {ndjson_path}")

if __name__ == "__main__":
    run_ingest_all()
    