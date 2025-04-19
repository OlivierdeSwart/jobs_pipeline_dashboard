from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# ✅ This is the magic line - put it BEFORE the import of your custom script
sys.path.append('/usr/local/airflow/dags/scripts')

# ✅ Now import your script function
from aws_1_api_to_storage import run_ingest_all

default_args = {
    'owner': 'olivier',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='aws_1_api_to_storage',
    default_args=default_args,
    description='Fetch jobs API and store in S3',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['jobs', 'api', 's3'],
) as dag:

    run_api_to_s3 = PythonOperator(
        task_id='run_api_to_storage',
        python_callable=run_ingest_all
    )

    run_api_to_s3