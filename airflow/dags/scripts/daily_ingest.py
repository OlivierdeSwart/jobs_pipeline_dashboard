import os
import json
import pandas as pd
from datetime import datetime, timedelta
import requests

def run_daily_ingest():
    url = "https://remotive.com/api/remote-jobs"
    response = requests.get(url)
    jobs = response.json()["jobs"]

    # Target = today - 2 for a stable full-day load
    target_date = (datetime.today() - timedelta(days=2)).date()
    folder_name = target_date.strftime("%Y-%m-%d")

    daily_jobs = [
        job for job in jobs
        if datetime.fromisoformat(job["publication_date"]).date() == target_date
    ]

    # Folder path reflects the target date
    folder = f"/opt/airflow/data_storage/remotive_job_api/{folder_name}/"
    os.makedirs(folder, exist_ok=True)

    # Save JSON
    json_path = os.path.join(folder, "daily.json")
    with open(json_path, "w") as f:
        json.dump(daily_jobs, f, indent=2)

    # Save CSV
    csv_path = json_path.replace(".json", ".csv")
    pd.DataFrame(daily_jobs).to_csv(csv_path, index=False)

    print(f"✅ Saved {len(daily_jobs)} jobs for {target_date} to:")
    print(f"  - {json_path}")
    print(f"  - {csv_path}")

if __name__ == "__main__":
    run_daily_ingest()
    