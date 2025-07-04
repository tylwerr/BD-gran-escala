import json
import pandas as pd
import glob
import pyarrow.parquet as pq
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import parquetio
from transform import transform_data, add_derived_metrics
from to_parquet import schema

# -------------------------------
# BEAM ETL
# -------------------------------

class ProcessRecord(beam.DoFn):
    def process(self, line):
        try:
            record = json.loads(line)

            #solo procesar si NO tiene datos de diagnóstico
            if record.get("Diagnostic_Data") is None:
                clean = transform_data(record)
                enriched = add_derived_metrics(clean)
                if enriched:
                    yield enriched
        except Exception:
            pass  #descartar línea malformada

def run_beam_pipeline(input_path, output_path, runner="DirectRunner", beam_args=None):
    options = PipelineOptions(beam_args)

    with beam.Pipeline(runner=runner, options=options) as pipeline:
        (
            pipeline
            | 'Read JSONL' >> beam.io.ReadFromText(input_path)
            | 'Process Record' >> beam.ParDo(ProcessRecord())
            | 'Write to Parquet' >> parquetio.WriteToParquet(
                file_path_prefix=output_path,
                schema=schema,
                file_name_suffix='.parquet',
                codec='gzip',
                num_shards=0
            )
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL para convertir JSONL de Sensores a Parquet.')
    parser.add_argument('--input_path', required=True, help='Ruta de entrada')
    parser.add_argument('--output_path', required=True, help='Ruta de salida')
    parser.add_argument('--runner', default='DirectRunner', help='Runner de Apache Beam, por ejemplo: "DirectRunner", "DataflowRunner", etc.')

    args, beam_args = parser.parse_known_args()
    input_path = args.input_path
    output_prefix = args.output_path

    run_beam_pipeline(input_path, output_prefix, runner=args.runner, beam_args=beam_args)
    
    #printear resultados
    try:
        parquet_files = glob.glob(f"{output_prefix}*.parquet") #generados por beam

        if parquet_files:
            df_result = pd.read_parquet(parquet_files)
            print(f"Total de filas procesadas por Beam: {len(df_result)}")
        else:
            print("¡Atención! No se encontraron archivos Parquet generados por Beam en el prefijo especificado.")
    except Exception as e:
        print(f"\nError al leer los archivos Parquet generados por Beam: {e}")
