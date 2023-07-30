from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.append("../etl/")
from etl.batch.app import upload_data_s3

default_args = {
    "owner": "Bifrost Analytics - Jess",
    "retries": 1,
    "retry_delay": 0 
}

with DAG(
    dag_id="dag_load_data_s3", 
    start_date=datetime(2023, 7, 28),
    schedule_interval=None,
    max_active_runs=1,
    default_args=default_args,
    catchup=False,
    tags=['bifrost','amazon-data', 'etl', 's3']
) as dag:
    
    upload_data_s3_task = PythonOperator(
        task_id='upload_data_s3',
        python_callable= upload_data_s3
    )

    upload_data_s3_task



