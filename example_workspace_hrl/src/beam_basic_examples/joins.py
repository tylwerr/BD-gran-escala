from apache_beam.dataframe.io import read_csv

import argparse
import logging
import re
import json
import pyarrow as pa
from datetime import datetime
from decimal import Decimal
import csv
import fastavro

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.avroio import WriteToAvro


class ConvertFn(beam.DoFn):
    def __init__(self, fieldnames):
        super().__init__()
        self.fieldnames = fieldnames

    def process(self, element):
        reader = csv.DictReader([element],
                                delimiter=';',
                                fieldnames=self.fieldnames)

        new_row = next(reader, None)

        # yield new_row
        return [new_row]


class EnrichDoFn(beam.DoFn):
    def process(self, element, zonas):
        element["zona"] = zonas.get(element["REGION"])
        yield element

def main(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the CSV file
        input_path_rural = 'data/rural.csv'
        rural_column_names = [
            "COD. REGIÓN",
            "REGIÓN",
            "COD. PROVINCIA",
            "PROVINCIA",
            "COD. COMUNA",
            "COMUNA",
            "NÚMERO DISTRITO",
            "COD. LOCALIDAD",
            "LOCALIDAD",
            "COD. ENTIDAD",
            "ENTIDAD",
            "CATEGORIA",
            "POBLACIÓN",
            "HOMBRES",
            "MUJERES",
            "VIVIENDAS"
        ]
        rural_full = (p
                      | 'Read rural CSV' >> beam.io.ReadFromText(input_path_rural, skip_header_lines=1)
                      | "Transform rural data" >> beam.ParDo(ConvertFn(rural_column_names))
                      )

        rural_filter = (rural_full
                        | "Filter rural" >> beam.Filter(
                            lambda row: row["CATEGORIA"] == "ALDEA"
                        )
                        | "Filter fields" >> beam.Map(
                            lambda row: {
                                "REGION": row["REGIÓN"],
                                "PROVINCIA": row["PROVINCIA"],
                                "COMUNA": row["COMUNA"],
                                "LOCALIDAD": row["ENTIDAD"],
                                "CATEGORIA": row["CATEGORIA"]
                            }
                        )
                        )

        input_path_urbano = 'data/urbano.csv'
        urbano_column_names = [
            "COD. REGIÓN",
            "COD. COMUNA",
            "REGIÓN",
            "PROVINCIA",
            "COMUNA",
            "URBANO",
            "TIPO",
            "CATEGORIA",
            "POBLACIÓN",
            "HOMBRES",
            "MUJERES",
            "VIVIENDAS"
        ]

        urbano_full = (p | 'Read urbano CSV' >> beam.io.ReadFromText(input_path_urbano, skip_header_lines=1)
                       | "Transform urbano data" >> beam.ParDo(ConvertFn(urbano_column_names))
                       )

        urbano_filter = (urbano_full
                         | "Filter urbano" >> beam.Map(
                             lambda row: {
                                 "REGION": row["REGIÓN"],
                                 "PROVINCIA": row["PROVINCIA"],
                                 "COMUNA": row["COMUNA"],
                                 "LOCALIDAD": row["URBANO"],
                                 "CATEGORIA": row["CATEGORIA"]
                             }
                         )
                         )

        # merge the two PCollections
        merged = ((rural_filter, urbano_filter)
                  | 'Merge PCollections' >> beam.Flatten()
                  )

        # Convert to JSON
        merged_json = merged | 'Convert to JSON' >> beam.Map(
            lambda row: json.dumps(row, ensure_ascii=False)
        )
        # Write to CSV
        output_path = 'output/localidades'
        merged_json | 'Write to JSON' >> beam.io.WriteToText(
            output_path,
            file_name_suffix='.json',
            num_shards=1
        )

        # Join the two PCollections
        urbano_comuna = urbano_full | 'Urbano by COMUNA' >> beam.Map(
            lambda row: (f"{row['REGIÓN']}:{row['PROVINCIA']}:{row['COMUNA']}", 
                         {
                            "LOCALIDAD": row["URBANO"],
                            "TIPO": row["CATEGORIA"],
                         })
        )
        rural_comuna = rural_full | 'rural by COMUNA' >> beam.Map(
            lambda row: (f"{row['REGIÓN']}:{row['PROVINCIA']}:{row['COMUNA']}", 
                         {
                            "LOCALIDAD": row["ENTIDAD"],
                            "TIPO": row["CATEGORIA"],
                         })
        )

        results = ({'zona_urbana': urbano_comuna,
                   'zona_rural': rural_comuna} | beam.CoGroupByKey())

        (results
            | 'Convert Join to JSON' >> beam.Map(
                lambda row: json.dumps(row, ensure_ascii=False)
            )
            | 'Write Join to JSON' >> beam.io.WriteToText(
                'output/localidades_por_comuna',
                file_name_suffix='.json',
                num_shards=1
            )
         )
        
        regiones_zonas = {
            'ARICA Y PARINACOTA': 'Norte',
            'TARAPACÁ': 'Norte',
            'ANTOFAGASTA': 'Norte',
            'ATACAMA': 'Norte',
            'COQUIMBO': 'Norte',
            'VALPARAÍSO': 'Centro',
            'REGIÓN METROPOLITANA DE SANTIAGO': 'Centro',
            'LIBERTADOR GENERAL BERNARDO O\'HIGGINS': 'Centro',
            'MAULE': 'Centro',
            'ÑUBLE': 'Centro',
            'BIOBÍO': 'Centro',
            'LA ARAUCANÍA': 'Sur',
            'LOS RÍOS': 'Sur',
            'LOS LAGOS': 'Sur',
            'AYSÉN DEL GENERAL CARLOS IBÁÑEZ DEL CAMPO': 'Sur',
            'MAGALLANES Y LA ANTÁRTICA CHILENA': 'Sur'
        }

        (
            merged
            | "Add zone" >> beam.ParDo(EnrichDoFn(), regiones_zonas)
            | 'Convert E to JSON' >> beam.Map(
                lambda row: json.dumps(row, ensure_ascii=False)
            )
            | 'Write E to JSON' >> beam.io.WriteToText(
                'output/localidades_con_zona',
                file_name_suffix='.json',
                num_shards=1
            )
        )

        # Write to Avro
        # output_path = '/workspaces/workspace/data/merged_data.avro'
        # merged | 'Write to Avro' >> WriteToAvro(
        #    output_path,
        #    schema = {
        #        'type': 'record',
        #        'name': 'Localidades',
        #        'fields': [
        #            {'name': 'REGION', 'type': 'string'},
        #            {'name': 'PROVINCIA', 'type': 'string'},
        #            {'name': 'COMUNA', 'type': 'string'},
        #            {'name': 'LOCALIDAD', 'type': 'string'}
        #        ]
        #    },
        #    file_name_suffix='.avro',
        #    codec='deflate'
        # )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
