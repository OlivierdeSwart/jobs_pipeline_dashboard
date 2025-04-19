#!/bin/bash

# Only install on worker (not scheduler or webserver)
if [[ "${MWAA_AIRFLOW_COMPONENT}" != "worker" ]]; then
    exit 0
fi

echo "🔧 Installing virtual Python environment for dbt..."

# Upgrade pip
pip3 install --upgrade pip

echo "🐍 Python version:"
python3 --version

# Install virtualenv if needed
pip3 install --user virtualenv

# Create and configure virtualenv inside MWAA writable dir
cd /usr/local/airflow
mkdir -p python3-virtualenv
cd python3-virtualenv
python3 -m venv dbt-env
chmod -R 777 *

echo "⚡ Activating venv..."
source dbt-env/bin/activate

echo "📦 Installing dbt packages..."
pip install dbt-core==1.6.1 dbt-snowflake==1.6.1

echo "📚 Installed packages:"
pip list

echo "🧪 DBT version:"
dbt --version

echo "🛑 Deactivating venv..."
deactivate