from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default DAG settings
default_args = {
    'owner': 'olivier',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Run at 09:00 UTC every day
with DAG(
    dag_id='daily_remotive_ingest',
    default_args=default_args,
    description='Daily job ingest from Remotive API (2-day delay strategy)',
    schedule_interval='0 9 * * *',
    start_date=datetime(2025, 4, 3),
    catchup=False,
    tags=['remotive', 'jobs', 'api', 'ingest']
) as dag:

    run_script = BashOperator(
        task_id='run_daily_ingest_script',
        bash_command='python /opt/airflow/dags/../dags/scripts/daily_ingest.py',
    )