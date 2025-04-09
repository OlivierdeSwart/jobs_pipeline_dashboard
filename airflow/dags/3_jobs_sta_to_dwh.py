# airflow/dags/dbt_jobs_base.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='3_jobs_sta_to_dwh',
    default_args=default_args,
    schedule_interval=None,  # Change to '@daily' or cron as needed
    catchup=False,
    tags=['dbt', 'jobs'],
) as dag:

    run_dbt_model = BashOperator(
        task_id='run_jobs_base',
        bash_command="""
            set -a
            source /opt/airflow/.env
            set +a
            cd /opt/airflow/dbt_jobs
            dbt run --select jobs_base --profiles-dir . --project-dir .
        """
    )
    