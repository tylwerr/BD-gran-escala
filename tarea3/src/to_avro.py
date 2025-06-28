import apache_beam as beam
import json
from datetime import datetime
import argparse
import logging
from pathlib import Path

#Configuracion basica de logging
logging.basicConfig(level=logging.INFO)

#Esquema avro
AVRO_SCHEMA = {
  "type": "record",
  "name": "FanEngagement",
  "fields": [
    {"name": "FanID", "type": "string"},
    {"name": "RaceID", "type": "string"},
    {"name": "Timestamp", "type": "string"},
    {"name": "Timestamp_unix", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "ViewerLocationCountry", "type": "string"},
    {"name": "DeviceType", "type": "string"},
    {"name": "EngagementMetric_secondswatched", "type": "int"},
    {"name": "PredictionClicked", "type": "boolean"},
    {"name": "MerchandisingClicked", "type": "boolean"}
  ]
}

def parse_json_line(line):
    """
    Parsea una linea JSON a un diccionario Python.
    """
    try:
        return json.loads(line)
    except json.JSONDecodeError as e:
        logging.error(f"Error decodificando JSON: {line}. Error: {e}")
        #Devolver None para filtrar o un valor por defecto
        return None

def add_unix_timestamp_and_validate(fan_data):
    """
    Calcula el Timestamp_unix en milisegundos y asegura que los tipos de datos
    coinciden con el esquema Avro.
    """
    if fan_data is None: #Manejar casos donde el parse_json_line devolvio None
        return None

    timestamp_str = fan_data.get('Timestamp') # 
    if not timestamp_str:
        logging.warning(f"Registro sin campo 'Timestamp', saltando: {fan_data}")
        return None

    try:
        dt_object = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')

        timestamp_unix_ms = int(dt_object.timestamp() * 1000)

        fan_data['Timestamp_unix'] = timestamp_unix_ms

        #Asegurar tipos correctos para Avro
        fan_data['EngagementMetric_secondswatched'] = int(fan_data.get('EngagementMetric_secondswatched', 0))
        fan_data['PredictionClicked'] = bool(fan_data.get('PredictionClicked', False))
        fan_data['MerchandisingClicked'] = bool(fan_data.get('MerchandisingClicked', False))

        return fan_data
    except ValueError as e:
        logging.error(f"Error en formato de fecha para '{timestamp_str}': {e}. Saltando registro: {fan_data}")
        return None
    except KeyError as e:
        logging.error(f"Campo faltante en el registro: {e}. Saltando registro: {fan_data}")
        return None


def run():
    parser = argparse.ArgumentParser(description='ETL para convertir JSON de Fans HRL a Avro.')
    parser.add_argument(
        '--input_path',
        dest='input_path',
        required=True,
        help='Ruta de los archivos JSON de entrada (puede ser local o un bucket).')
    parser.add_argument(
        '--output_path',
        dest='output_path',
        required=True,
        help='Ruta de salida para los archivos Avro (puede ser local o un bucket).')
    parser.add_argument(
        '--runner',
        dest='runner',
        default='DirectRunner', # DirectRunner para pruebas locales
        help='El runner de Beam a utilizar (e.g., DirectRunner, DataflowRunner).')
    
    args, beam_args = parser.parse_known_args()

    #Configuracion de las opciones del pipeline de Beam
    beam_options = beam.options.pipeline_options.PipelineOptions(beam_args)
    input_path = str(Path(args.input_path).resolve()) #para resolver error con la ruta LOCAL

    with beam.Pipeline(runner=args.runner, options=beam_options) as pipeline:
        #Leer los archivos JSON de entrada
        (
            pipeline
            | 'Read JSONL' >> beam.io.ReadFromText(input_path)
            #Parsear cada linea JSON a un diccionario
            | 'Parse JSON' >> beam.Map(parse_json_line)
            #Filtrar registros que no pudieron ser parseados o validados
            | 'Filter None Records' >> beam.Filter(lambda x: x is not None)
            #Colocar el campo 'Timestamp_unix' y validar tipos
            | 'Add Unix Timestamp & Validate' >> beam.Map(add_unix_timestamp_and_validate)
            #Volver a filtrar por si la transformacion o validacion devolvio None
            | 'Filter None After Transformation' >> beam.Filter(lambda x: x is not None)
            #Escribir la PCollection resultante a archivos Avro
            | 'Write to Avro' >> beam.io.WriteToAvro(
                file_path_prefix=args.output_path,
                schema=AVRO_SCHEMA,
                file_name_suffix='.avro',
                num_shards=1 #Numero de archivos de salida
            )
        )

if __name__ == '__main__':
    logging.info("Iniciando la ETL de JSON a Avro con Apache Beam.")
    run()
    logging.info("ETL de JSON a Avro con Apache Beam finalizada.")