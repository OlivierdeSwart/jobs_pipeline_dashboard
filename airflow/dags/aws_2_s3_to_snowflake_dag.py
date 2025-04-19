from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add the scripts folder to path
sys.path.append('/opt/airflow/dags/scripts')
from aws_2_s3_to_snowflake import load_s3_to_snowflake

default_args = {
    'owner': 'olivier',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='aws_2_s3_to_snowflake',
    default_args=default_args,
    description='Load NDJSON from S3 to Snowflake',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['snowflake', 's3', 'etl'],
) as dag:

    run_s3_to_sf = PythonOperator(
        task_id='run_s3_to_snowflake',
        python_callable=load_s3_to_snowflake
    )

    run_s3_to_sf  # 🧠 ← this line registers the task