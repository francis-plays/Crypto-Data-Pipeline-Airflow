import sys
import os
from pathlib import Path

# Add project root to path so we can import scripts
project_root = "/Users/francis/Desktop/Data Engineering/Crypto-Data-Pipeline-Airflow"
sys.path.insert(0, project_root)


from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Import the run functions from our scripts
from scripts.ingest import run as run_ingest
from scripts.clean import run as run_clean
from scripts.load import run as run_load

# Configure logging
log = logging.getLogger(__name__)

# Default arguments for all tasks
default_args = {
    'owner': 'francis',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 25),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

# Define the DAG
with DAG(
    'crypto_price_pipeline',
    default_args=default_args,
    schedule='*/0 * * * *',  # Every 1h  for testing
    catchup=False,
    description='Hourly crypto price pipeline: CoinGecko → S3 → Snowflake',
    tags=['crypto', 'etl', 'hourly'],
) as dag:

    # Task 1: Ingest from CoinGecko API to S3
    ingest_task = PythonOperator(
        task_id='ingest_prices',
        python_callable=run_ingest,
    )

    # Task 2: Clean data from S3 raw to S3 cleaned
    clean_task = PythonOperator(
        task_id='clean_and_score',
        python_callable=run_clean,
    )

    # Task 3: Load cleaned data from S3 to Snowflake
    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=run_load,
    )

    # Task dependencies: ingest → clean → load
    ingest_task >> clean_task >> load_task