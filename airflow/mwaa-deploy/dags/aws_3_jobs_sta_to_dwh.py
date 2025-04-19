from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import boto3
import json

def get_dbt_env_vars():
    secret_name = "one-secret-to-rule-them-all"
    region_name = "us-east-1"

    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])

    env_vars = {}

    # Export Snowflake creds
    for k, v in secret["snowflake"].items():
        env_vars[f"DBT_{k.upper()}"] = str(v)

    # Export DBT config
    for k, v in secret["dbt"].items():
        env_vars[f"DBT_{k.upper()}"] = str(v)

    return env_vars

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='aws_3_jobs_sta_to_dwh',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'jobs'],
) as dag:

    # Fetch secrets once at DAG parse time (they’ll be injected below)
    dbt_env = get_dbt_env_vars()

    run_dbt = BashOperator(
        task_id='run_jobs_base',
        bash_command="""
            cd /opt/airflow/dags/dbt_jobs
            dbt run --select jobs_base --profiles-dir . --project-dir .
        """,
        env=dbt_env  # 👈 pass secrets into BashOperator here
    )