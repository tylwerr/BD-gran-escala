from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from datetime import timedelta
import json
from kafka import KafkaProducer

# Globales

DAG_NAME = 'hrl_fan_engagement_etl'
BEAM_ETL_COMMAND = 'python to_avro.py --input_path ../input/*.json --output_path ../output/'
OUTPUT_LOCATION = '../output/'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'Our_topic'

def notify_kafka():
    processed_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    message = {
        "event_type": "data_processing_completed",
        "data_entity": "FanEngagement",
        "status": "success",
        "location": OUTPUT_LOCATION,
        "processed_at": processed_timestamp,
        "source_system": DAG_NAME
    }

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send(KAFKA_TOPIC, message)
    producer.flush()
    producer.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = DAG_NAME,
    default_args=default_args,
    description='ETL para convertir datos de participacion de fans de HRL de JSON a Avro',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['beam', 'kafka', 'json'],
) as dag:
    
    # Tarea 1: Ejecutar la ETL de Apache Beam
    run_beam_etl = BashOperator(
        task_id='run_beam_etl',
        bash_command = BEAM_ETL_COMMAND,
    )

    # Tarea 2: Notificar a Kafka
    notify_kafka_job = PythonOperator(
        task_id='notify_kafka',
        python_callable=notify_kafka,
    )

    run_beam_etl >> notify_kafka_job