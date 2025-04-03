import os
import json
import pandas as pd
from datetime import datetime, timedelta
import requests

def build_historical_dataset():
    url = "https://remotive.com/api/remote-jobs"
    response = requests.get(url)
    jobs = response.json()["jobs"]

    # Define cutoff: anything <= today - 3 is considered historical
    cutoff_date = (datetime.today() - timedelta(days=3)).date()
    folder_name = cutoff_date.strftime("%Y-%m-%d")

    historical_jobs = [
        job for job in jobs
        if datetime.fromisoformat(job["publication_date"]).date() <= cutoff_date
    ]

    # Folder path reflects the cutoff date
    folder = f"data_storage/remotive_job_api/{folder_name}/"
    os.makedirs(folder, exist_ok=True)

    # Save JSON
    json_path = os.path.join(folder, "historical_dataset.json")
    with open(json_path, "w") as f:
        json.dump(historical_jobs, f, indent=2)

    # Save CSV
    csv_path = json_path.replace(".json", ".csv")
    pd.DataFrame(historical_jobs).to_csv(csv_path, index=False)

    print(f"✅ Saved {len(historical_jobs)} historical jobs to:")
    print(f"  - {json_path}")
    print(f"  - {csv_path}")

if __name__ == "__main__":
    build_historical_dataset()
    