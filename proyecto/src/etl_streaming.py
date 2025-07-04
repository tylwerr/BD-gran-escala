import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io import parquetio
from transform import transform_data, add_derived_metrics
from to_parquet import schema

# -------------------------------
# BEAM ETL
# -------------------------------

class ProcessRecord(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.value.decode("utf-8"))  #mensaje desde Kafka

            #solo datos críticos
            if record.get("Diagnostic_Data"):
                clean = transform_data(record)
                enriched = add_derived_metrics(clean)
                if enriched:
                    yield enriched
        except Exception:
            pass  #descartar línea malformada


def run_streaming_pipeline(bootstrap_servers, topic, output_path, beam_args=None):
    options = PipelineOptions(beam_args)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read from Kafka" >> ReadFromKafka(
                consumer_config={"bootstrap.servers": bootstrap_servers},
                topics=[topic]
            )
            | "Process critical data" >> beam.ParDo(ProcessRecord())
            | "Write to Parquet" >> parquetio.WriteToParquet(
                file_path_prefix=output_path,
                schema=schema,
                file_name_suffix=".parquet",
                codec="gzip",
                num_shards=1  #según particiones
            )
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL streaming desde Kafka a Parquet.")
    parser.add_argument('--bootstrap_servers', required=True, help='Dirección del broker Kafka, ej: localhost:9092')
    parser.add_argument('--topic', required=True, help='Nombre del tópico Kafka')
    parser.add_argument('--output_path', required=True, help='Ruta de salida de archivos Parquet')
    args, beam_args = parser.parse_known_args()

    run_streaming_pipeline(args.bootstrap_servers, args.topic, args.output_path, beam_args)