from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='hrl_fan_engagement_etl', #Nombre unico para el DAG
    default_args=default_args,
    description='ETL para convertir datos de participacion de fans de HRL de JSON a Avro',
    schedule_interval=timedelta(days=1), #Se ejeutara diariamente
    start_date=days_ago(1), #Fecha de inicio, por ejemplo, ayer
    tags=['hrl', 'etl', 'beam', 'kafka'],
) as dag:
    # Tarea 1: Ejecutar la ETL de Apache Beam
    run_beam_etl = BashOperator(
        task_id='run_beam_etl',
        # Aquí llamarías a tu script de Apache Beam.
        # Asegúrate de que el script pueda tomar parámetros para las rutas de entrada/salida.
        # Por ejemplo: python /path/to/your_beam_etl.py --input_path /data/json --output_path /data/avro
        bash_command='python /opt/airflow/dags/scripts/beam_etl.py --input_path /opt/airflow/data/input/fan_data.jsonl --output_path /opt/airflow/data/output/fan_engagement.avro',
    )

        # Tarea 2: Notificar a Kafka
    # Necesitarás un script o un PythonOperator que envíe el mensaje a Kafka
    # y calcule processed_timestamp y path_or_bucket.
    notify_kafka = BashOperator(
        task_id='notify_kafka',
        bash_command='python /opt/airflow/dags/scripts/kafka_notifier.py '
                     '--event_type "data_processing_completed" '
                     '--data_entity "FanEngagement" '
                     '--status "success" '
                     '--location "/opt/airflow/data/output/fan_engagement.avro" ' # Ajusta esto dinámicamente si es necesario
                     '--pipeline_name "{{ dag.dag_id }}"',
    )