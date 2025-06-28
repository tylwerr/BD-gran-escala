from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import json


# Configuraciones globales
DAG_NAME = 'hrl_fan_engagement_etl'
SRC_DIR = '/tarea3/src'
INPUT_PATH = '/tarea3/input/*.json'
OUTPUT_PATH = '/tarea3/output/'
BEAM_ETL_COMMAND = f'python {SRC_DIR}/to_avro.py --input_path {INPUT_PATH} --output_path {OUTPUT_PATH}'
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_TOPIC = 'hrl-fan-engagement'


def ensure_kafka_topic(topic_name, bootstrap_servers):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    existing_topics = admin_client.list_topics(timeout=10).topics

    if topic_name not in existing_topics:
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()
                print(f'Tópico "{topic}" creado con éxito.')
            except Exception as e:
                print(f'Error al crear el tópico "{topic}": {e}')
    else:
        print(f'Tópico "{topic_name}" ya existe.')


def notify_kafka():
    processed_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    message = {
        "event_type": "data_processing_completed",
        "data_entity": "FanEngagement",
        "status": "success",
        "location": OUTPUT_PATH,
        "processed_at": processed_timestamp,
        "source_system": DAG_NAME
    }

    ensure_kafka_topic(KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)

    def callback(err, msg):
        if err is not None:
            print(f"Error al enviar mensaje: {err}")
        else:
            print(f"Mensaje enviado a {msg.topic()} [{msg.partition()}]")

    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    producer.produce(KAFKA_TOPIC, value=json.dumps(message), callback=callback)
    producer.flush()


# Definición del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id=DAG_NAME,
    start_date=datetime(2025, 1, 1),
    description='ETL para convertir datos de participación de fans de HRL de JSON a Avro',
    schedule='@daily',
    catchup=False,
    tags=['beam', 'kafka', 'json'],
    default_args=default_args,
):

    # Tarea 1: Ejecutar Beam ETL
    run_beam_etl = BashOperator(
        task_id='run_beam_etl',
        bash_command=BEAM_ETL_COMMAND
    )

    # Tarea 2: Enviar mensaje a Kafka
    notify_kafka_task = PythonOperator(
        task_id='notify_kafka',
        python_callable=notify_kafka
    )

    #primera tarea >> segunda tarea
    run_beam_etl >> notify_kafka_task
