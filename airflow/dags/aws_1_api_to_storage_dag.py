from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow/dags/scripts')

from aws_1_api_to_storage import run_ingest_all  # <- updated this to match your script

default_args = {
    'owner': 'olivier',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='aws_1_api_to_storage',
    default_args=default_args,
    description='Fetch jobs API and store in S3',
    schedule_interval='@daily',
    start_date=datetime(2024, 4, 1),
    catchup=False,
    tags=['jobs', 'api', 's3'],
) as dag:

    run_api_to_s3 = PythonOperator(
        task_id='run_api_to_storage',
        python_callable=run_ingest_all  # <- must point to the function name in script
    )

    run_api_to_s3  # <- this line is required