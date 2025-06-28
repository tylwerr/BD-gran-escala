import argparse
import logging
import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions, StandardOptions
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.transforms.trigger import AfterProcessingTime, Repeatedly, AfterWatermark
from apache_beam.transforms.trigger import AccumulationMode
import json
import os
import boto3
from botocore.config import Config
from apache_beam.io.kafka import WriteToKafka
import uuid

# --- Configuración de MinIO (compatible con S3) ---
MINIO_ENDPOINT = 'http://ws-minio:9000'
MINIO_ACCESS_KEY = 'minio-root-user' # Credenciales por defecto de MinIO
MINIO_SECRET_KEY = 'minio-root-password' # Credenciales por defecto de MinIO
MINIO_BUCKET_NAME = 'beam-output-bucket' # Nombre del bucket donde se escribirán los datos
OUTPUT_FILE_PREFIX = 'output/data' # Prefijo del archivo de salida dentro del bucket

# Configuración de las variables de entorno para boto3
# Esto permite que boto3 se conecte a MinIO en lugar de AWS S3
os.environ['AWS_ACCESS_KEY_ID'] = MINIO_ACCESS_KEY
os.environ['AWS_SECRET_ACCESS_KEY'] = MINIO_SECRET_KEY
os.environ['AWS_REGION_NAME'] = 'us-east-1' # La región es arbitraria para MinIO local
os.environ['AWS_ENDPOINT_URL'] = MINIO_ENDPOINT

# --- Función para inicializar el cliente S3 (boto3) ---
def get_s3_client():
    """
    Crea y retorna un cliente S3 configurado para MinIO.
    """
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        verify=False # Necesario si MinIO usa un certificado autofirmado o HTTP
    )

# --- Función para asegurar que el bucket exista ---
def ensure_minio_bucket_exists(bucket_name):
    """
    Verifica si un bucket existe en MinIO y lo crea si no.
    """
    s3_client = get_s3_client()
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"El bucket '{bucket_name}' ya existe en MinIO.")
    except s3_client.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            logging.info(f"El bucket '{bucket_name}' no existe, creándolo...")
            s3_client.create_bucket(Bucket=bucket_name)
            logging.info(f"Bucket '{bucket_name}' creado exitosamente en Minio.")
        else:
            logging.error(f"Error al verificar/crear el bucket '{bucket_name}': {e}")
            raise

def main(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--main_prefix',
        dest='main_prefix',
        required=True,
        help='Prefix for all output')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True
    #pipeline_options.view_as(AwsOptions).setAwsServiceEndpoint("http://localhost:9000");

    ensure_minio_bucket_exists(MINIO_BUCKET_NAME)

    with beam.Pipeline(options=pipeline_options) as p:

        def format_as_json(element):
            if isinstance(element, tuple):
                return element[1].decode('utf-8')
            else:
                raise RuntimeError('unknown record type: %s' % type(element))

        output_path = f"s3://{MINIO_BUCKET_NAME}/{OUTPUT_FILE_PREFIX}"
        bootstrap_servers = 'ws-kafka:19092'
        records = (
            p
            # Read messages from an Apache Kafka topic.
            | ReadFromKafka(
                consumer_config={'bootstrap.servers': bootstrap_servers,
                                 'auto.offset.reset': 'earliest'},
                topics=['race_monitoring_topic'],
                max_num_records=10  # for testing purposes
            )
            | 'FormatOutputToJson' >> beam.Map(format_as_json)
        )
        (records
            # | 'ToJsonString' >> beam.Map(json.dumps)
            # Subdivide the output into fixed 5-second windows.
            | beam.WindowInto(window.FixedWindows(5))
            | beam.io.WriteToText(output_path, file_name_suffix='.json')
        )

        (records
         | 'To KVs' >> beam.Map(lambda x: (str(uuid.uuid4()).encode('utf-8'), x.encode('utf-8')))
         .with_output_types(tuple[bytes, bytes])
         | WriteToKafka(
            producer_config={'bootstrap.servers': bootstrap_servers},
            topic=MINIO_BUCKET_NAME)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
