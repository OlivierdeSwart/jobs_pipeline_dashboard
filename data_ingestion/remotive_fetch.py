# data_ingestion/remotive_fetch.py

import requests
import json
from datetime import datetime

def fetch_jobs():
    url = "https://remotive.com/api/remote-jobs"
    response = requests.get(url)
    jobs = response.json()["jobs"]

    today = datetime.today().strftime("%Y-%m-%d")
    filename = f"remotive_jobs_{today}.json"

    with open(filename, "w") as f:
        json.dump(jobs, f, indent=2)

    print(f"✅ Saved {len(jobs)} jobs to {filename}")

if __name__ == "__main__":
    fetch_jobs()
    