from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import dbt.version

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

def python_check_dbt():
    print("🧪 Checking dbt via Python...")
    version_info = dbt.version.get_installed_version()
    print(f"✅ dbt-core version: {version_info}")
    # You can also access version_info.to_dict() for more details

with DAG(
    dag_id='debugging_dag_1',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'debug'],
) as dag:

    # CLI check via BashOperator (not ideal for production)
    bash_check_dbt = BashOperator(
        task_id='verify_dbt_version_bash',
        bash_command="""
            echo "🧪 Trying to run: dbt --version"
            dbt --version || echo '❌ dbt not found'
        """
    )

    # Python check via dbt-core
    python_check = PythonOperator(
        task_id='verify_dbt_version_python',
        python_callable=python_check_dbt
    )

    bash_check_dbt >> python_check