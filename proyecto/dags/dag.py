from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime

# Parámetros
INPUT_PATH = '/proyecto/data/*.json'
OUTPUT_PATH = '/proyecto/output/'
PYTHON_SCRIPT_PATH = '/proyecto/src/etl_batch.py'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_jsonl_to_parquet',
    default_args=default_args,
    description='Pipeline ETL con Apache Beam (local) para convertir JSONL a Parquet',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'beam', 'parquet'],
) as dag:

    run_beam_etl = BashOperator(
        task_id='run_beam_etl',
        bash_command=f'python3 {PYTHON_SCRIPT_PATH} --input_path {INPUT_PATH} --output_path {OUTPUT_PATH} --runner DirectRunner',
    )

    run_beam_etl
