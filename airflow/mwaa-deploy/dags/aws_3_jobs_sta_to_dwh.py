from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import boto3
import json

# 🔐 Fetch secrets from AWS Secrets Manager for dbt + Snowflake
def get_dbt_env():
    secret_name = "one-secret-to-rule-them-all"
    region_name = "us-east-1"

    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])

    env_vars = {}

    # Prefix vars so dbt picks them up as environment variables
    for k, v in secret["snowflake"].items():
        env_vars[f"DBT_{k.upper()}"] = str(v)

    for k, v in secret["dbt"].items():
        env_vars[f"DBT_{k.upper()}"] = str(v)

    # ✅ Write logs to ephemeral /tmp to avoid cost/log spam
    env_vars["DBT_LOG_PATH"] = "/tmp/dbt.log"

    return env_vars

# DAG args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

# Define DAG
with DAG(
    dag_id='aws_3_jobs_sta_to_dwh',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'jobs'],
) as dag:

    run_dbt = BashOperator(
        task_id='run_dbt_jobs_base',
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