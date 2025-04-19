from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import json

def test_secrets_user():
    secret_name = "one-secret-to-rule-them-all"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    client = boto3.client("secretsmanager", region_name=region_name)

    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])

    # Grab only the Snowflake "user"
    user = secret.get("snowflake", {}).get("user")

    if user:
        print(f"✅ Snowflake user: {user}")
    else:
        print("❌ Could not find Snowflake user in secret.")

with DAG(
    dag_id="test_secrets_user",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "secrets"]
) as dag:

    t1 = PythonOperator(
        task_id="read_user_from_secret",
        python_callable=test_secrets_user
    )