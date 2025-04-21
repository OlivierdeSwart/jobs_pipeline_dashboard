# 🛠 JOBS_PIPELINE_DASHBOARD

End-to-end modern data engineering pipeline to ingest job postings, transform with dbt, and visualize insights in Tableau. Deployable locally via Docker or in production on AWS using MWAA (Managed Workflows for Apache Airflow), S3, and Snowflake.

---

## 🔧 Project Structure

JOBS_PIPELINE_DASHBOARD/
│
├── airflow/                    # Local Airflow project root
│   ├── dags/                   # Local-only DAGs
│   ├── config/                 # Configuration files
│   └── logs/                   # Airflow logs
│
├── mwaa-deploy/                # Production deployable setup for AWS MWAA
│   ├── dags/
│   │   ├── dbt/                # dbt project directory
│   │   └── scripts/           # Python scripts used in the pipeline
│   ├── requirements.txt        # MWAA dependencies
│   └── startup.sh              # Bootstrapping shell script
│
├── dbt_jobs/                   # dbt project
│   ├── models/
│   │   ├── staging/            # Stage raw Snowflake tables
│   │   ├── dwh/                # Data warehouse layer
│   │   └── dm/                 # Data marts
│   ├── snapshots/              # Versioning for slowly changing dimensions
│   ├── logs/                   # dbt logs
│   ├── seeds/                  # Static CSV seed files
│   └── dbt_project.yml         # Project config
│
├── data_storage/               # Ingested NDJSON data from API
├── data_analysis/              # Notebook-style EDA or analysis scripts
├── scripts/                    # Finalized ingestion/transformation scripts
├── scripts_old/                # Deprecated/archived versions
├── logs/Screenshots/           # Pipeline and dashboard screenshots
│
├── .env                        # Environment variables
├── docker-compose.yaml         # Local dev stack
├── Dockerfile                  # Airflow image config
├── requirements.txt            # Python dependencies
├── README.md                   # This file ✨
└── 20250403_notes              # Project journal / scratchpad

---

## ⚙️ How It Works

### 1. Ingest Jobs from API → S3
- `aws_1_api_to_storage.py`
- PythonOperator calls `run_ingest_all()` from `scripts/aws_1_api_to_storage.py`
- Stores raw NDJSON in S3 (or locally in dev mode)

### 2. Load S3 Data → Snowflake
- `aws_2_s3_to_snowflake.py`
- PythonOperator calls `load_s3_to_snowflake()` with environment vars from `.env`
- Loads data into Snowflake raw tables

### 3. dbt: Transform in Snowflake
- `aws_3_jobs_sta_to_dwh.py`
- BashOperator calls dbt using secrets from AWS Secrets Manager
- Models include:
  - `jobs_base.sql`: Raw jobs cleaned
  - `jobs_current.sql`: Current open jobs
  - `fct_job_postings_lifecycle.sql`: New, updated, deleted logic

### 4. Combine in a Single DAG
- `aws_4_all_combined.py`
- Runs the full daily flow: API → S3 → Snowflake → dbt

---

## 🚀 Deployment

### ▶️ Local (Docker Compose)
```bash
docker-compose up airflow-init
docker-compose up

	•	Airflow UI: http://localhost:8080
	•	Dags are mounted live from airflow/dags (dev) or mwaa-deploy/dags (prod)

☁️ AWS (MWAA)
	1.	Package up mwaa-deploy/ contents
	2.	Upload to S3 bucket configured for your MWAA environment
	3.	Ensure:
	•	.env and secrets are in AWS Secrets Manager
	•	Your Airflow environment has access to S3, Snowflake, and Secrets Manager
	4.	Use startup.sh for bootstrapping

⸻

📊 Dashboard

Built in Tableau using Snowflake as a live source.

Key dashboards:
	•	Jobs Lifecycle: New, updated, archived jobs per day
	•	Jobs KPIs: Avg open days, new jobs per day
	•	Behavioral cohort potential via job updates

⸻

🧠 Notebooks, Notes, and Screenshots

Located in:
	•	data_analysis/ – EDA + Tableau prep
	•	20250403_notes – scratchpad for pipeline logic
	•	logs/Screenshots/ – proof of success + dashboard output

⸻

🪪 Auth & Secrets

Secrets pulled securely from AWS Secrets Manager:
	•	one-secret-to-rule-them-all – contains Snowflake + dbt creds

⸻

📦 Future Improvements
	•	Add CI/CD for dbt with GitHub Actions
	•	Incremental model logic
	•	Snapshot coverage for full lifecycle tracking
	•	Behavioral cohorts on job updates
	•	Webhook/streaming ingestion

⸻

🧠 Author

Made by Olivier de Swart
olivierdeswart.com

⸻

Screenshot
Description
05_airflow_dag_success.png
Successful local DAG execution
06_snowflake_first_ingested_data.png
First records in raw Snowflake table
07_dbt_models_done.png
dbt models compiled & run
10_tableau_viz.png
Final dashboard
13_dashboard_lifecycle.png
Lifecycle viz showing job states
