from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default DAG settings
default_args = {
    'owner': 'olivier',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Run at 09:00 UTC every day
with DAG(
    dag_id='2_jobs_to_snowflake',
    default_args=default_args,
    description='Daily job ingest from Remotive API (2-day delay strategy)',
    # schedule_interval='0 15 * * *',
    schedule_interval='04 13 * * *',
    start_date=datetime(2025, 4, 3),
    catchup=False,
    tags=['remotive', 'jobs', 'api', 'ingest']
) as dag:

    # run_script = BashOperator(
    #     task_id='run_load_daily_to_snowflake_2_script',
    #     bash_command='python /opt/airflow/dags/../dags/scripts/load_daily_to_snowflake_2.py',
    # )

    run_script = BashOperator(
        task_id='run_load_daily_to_snowflake_2_script',
        # bash_command='set -a && source /opt/airflow/.env && python /opt/airflow/dags/scripts/load_daily_to_snowflake_2.py',
        bash_command='bash -c "set -a && source /opt/airflow/.env && python /opt/airflow/dags/scripts/load_daily_to_snowflake_2.py"'
    )