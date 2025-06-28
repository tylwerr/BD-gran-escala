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


class ConvertDemoFn(beam.DoFn):
    def __init__(self, label=None):
        super().__init__()

    def process(self, element):
        reader = csv.DictReader([element],
                                delimiter=',',
                                fieldnames=['region','pob_regional','provincia','comuna','pob_comunal','sup_comuna_km2','densidad_hab_x_km2'])

        new_row = next(reader, None)

        yield new_row
        #return [new_row]

def main(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        def format_as_json(element):
            """Formats a key-value pair as a JSON string."""
            key, value = element
            #print(json.dumps({key: value}))
            return json.dumps({key: value}, ensure_ascii=False).encode('utf8')

        input_demographics_path = 'data/chiledemografico_por_comuna.csv'
        lines_demo = (p | 'Read Demo CSV' >> beam.io.ReadFromText(input_demographics_path, skip_header_lines=1)
                 | "Transform Demo data" >> beam.ParDo(ConvertDemoFn())
                 | 'Map Demo to Key-Value' >> beam.Map(lambda element: (element['comuna'], element))
                 | 'Format Demo Output' >> beam.Map(format_as_json)
                 | 'Write Demo 1' >> beam.io.WriteToText(file_path_prefix='output/comunas por ciudad'
                                   , file_name_suffix='.json'
                                   )
                 )




if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
