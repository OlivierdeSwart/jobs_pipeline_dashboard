from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'olivier',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='4_all_combined_jobs_pipeline',
    default_args=default_args,
    description='Combined pipeline: API -> storage -> Snowflake -> dbt',
    schedule_interval='0 13 * * *',  # run daily at 13:00 UTC
    start_date=datetime(2025, 4, 3),
    catchup=False,
    tags=['jobs', 'pipeline', 'dbt']
) as dag:

    step_1_api_to_storage = BashOperator(
        task_id='step_1_api_to_storage',
        bash_command='python /opt/airflow/dags/scripts/1_api_to_storage.py'
    )

    step_2_storage_to_snowflake = BashOperator(
        task_id='step_2_storage_to_snowflake',
        bash_command='bash -c "set -a && source /opt/airflow/.env && python /opt/airflow/dags/scripts/2_storage_to_snowflake.py"'
    )

    step_3_run_dbt = BashOperator(
        task_id='step_3_run_dbt',
        bash_command="""
            set -a
            source /opt/airflow/.env
            set +a
            cd /opt/airflow/dbt_jobs
            dbt run --select jobs_base+ --profiles-dir . --project-dir .
        """
    )

    step_1_api_to_storage >> step_2_storage_to_snowflake >> step_3_run_dbt
    