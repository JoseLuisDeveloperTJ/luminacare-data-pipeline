from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
from src.ingestion.ingest_lumina_tickets import test_upload

sys.path.append('/usr/local/airflow')

try:
    from src.ingestion.ingest_lumina_tickets import test_upload
except ImportError as e:
    
    print(f"Importing error: {e}")

default_args = {
    'owner': 'Luis_DE',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_lumina_ingestion_s3',
    default_args=default_args,
    description='Ingest Automation SaaS a S3 Raw',
    schedule='@hourly',
    start_date=datetime(2026, 3, 17),
    catchup=False,
    tags=['luminacare', 'ingestion']
) as dag:

    task_ingesta = PythonOperator(
        task_id='execute_ingest_python',
        python_callable=test_upload,
    )