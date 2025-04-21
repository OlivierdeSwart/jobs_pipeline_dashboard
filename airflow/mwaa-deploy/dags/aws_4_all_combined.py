from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import boto3
import json
import sys

# 🧠 Make your scripts importable
sys.path.append('/usr/local/airflow/dags/scripts')

# ✅ Import Python callables from your scripts
from aws_1_api_to_storage import run_ingest_all
from aws_2_s3_to_snowflake import load_s3_to_snowflake

# 🔐 Fetch secrets from AWS Secrets Manager for dbt + Snowflake
def get_dbt_env():
    secret_name = "one-secret-to-rule-them-all"
    region_name = "us-east-1"

    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])

    env_vars = {}

    for k, v in secret["snowflake"].items():
        env_vars[f"DBT_{k.upper()}"] = str(v)

    for k, v in secret["dbt"].items():
        env_vars[f"DBT_{k.upper()}"] = str(v)

    env_vars["DBT_LOG_PATH"] = "/tmp/dbt.log"  # Logs to ephemeral storage
    return env_vars

# 🧱 DAG args
default_args = {
    'owner': 'olivier',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# 🧪 DAG definition
with DAG(
    dag_id='aws_4_all_combined_jobs_pipeline',
    default_args=default_args,
    description='Combined pipeline: API -> S3 -> Snowflake -> dbt',
    schedule_interval='0 08 * * *',  # Daily at 13:00 UTC
    start_date=datetime(2025, 4, 3),
    catchup=False,
    tags=['jobs', 'pipeline', 'dbt']
) as dag:

    # 🟢 Step 1: API to S3
    step_1_api_to_storage = PythonOperator(
        task_id='step_1_api_to_storage',
        python_callable=run_ingest_all
    )

    # 🔵 Step 2: S3 to Snowflake
    step_2_storage_to_snowflake = PythonOperator(
        task_id='step_2_storage_to_snowflake',
        python_callable=load_s3_to_snowflake
    )

    # 🔶 Step 3: dbt transformation
    step_3_run_dbt = BashOperator(
        task_id='step_3_run_dbt',
        bash_command="""
        echo "📂 Changing to dbt project directory"
        cd /usr/local/airflow/dags/dbt/dbt_jobs

        echo "🚀 Running dbt using full path with safe target path"
/usr/local/airflow/.local/bin/dbt run \
    --select jobs_base+ \
    --project-dir . \
    --profiles-dir . \
    --target-path /tmp/dbt_target

        echo "🧹 Skipping dbt log cleanup (handled by tmp lifecycle)"
        """,
        env=get_dbt_env()
    )

    # 🔗 Pipeline flow
    step_1_api_to_storage >> step_2_storage_to_snowflake >> step_3_run_dbt