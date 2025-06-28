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


class _OutputFn(beam.DoFn):
    def __init__(self, label=None):
        super().__init__()

    def process(self, element):
        reader = csv.DictReader([element],
                                delimiter=',',
                                fieldnames=['UTC_Date', 'Profundity', 'Magnitude', 'Date', 'Hour', 'Location', 'Latitude', 'Longitude'])

        new_row = next(reader, None)
        # UTC Date
        datetime_string = new_row["UTC_Date"]
        datetime_obj = datetime.strptime(
            datetime_string, "%Y-%m-%d %H:%M:%S")
        # datetime_obj.timestamp()  * 1000
        new_row["UTC_Date"] = datetime_obj
        # Date
        datetime_string = new_row["Date"]
        datetime_obj = datetime.strptime(
            datetime_string, "%Y-%m-%d").date()
        new_row["Date"] = datetime_obj
        # Hour
        time_obj = datetime.strptime(new_row["Hour"], "%H:%M:%S").time()
        new_row["Hour"] = time_obj
        new_row["Latitude"] = Decimal(new_row["Latitude"])
        new_row["Longitude"] = Decimal(new_row["Longitude"])
        
        #yield new_row
        return [new_row]

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
        # Read the CSV file
        input_path = 'data/EarthquakesChile_2000-2024.csv'
        lines = (p | 'Read CSV' >> beam.io.ReadFromText(input_path, skip_header_lines=1)
                 | "Transform data" >> beam.ParDo(_OutputFn())
                 )
        

        def format_as_json(element):
            """Formats a key-value pair as a JSON string."""
            key, value = element
            #print(json.dumps({key: value}))
            return json.dumps({key: value}, ensure_ascii=False).encode('utf8')

        # obtener cantidad de sismos por ciudad
        (lines
             | 'Get city' >> beam.Map(lambda x: x['Location'].split("de ",1)[1])
             | 'Count words' >> beam.combiners.Count.PerElement()
             | 'FormatOutput' >> beam.Map(format_as_json)
             | beam.io.WriteToText(file_path_prefix='output/sismos por ciudad'
                                   , file_name_suffix='.json'
                                   )
        )

        # filtrar sismos solo con escala Richter
        (lines
             | beam.Filter(lambda x: x['Magnitude'].endswith("Ml"))
             #| 'Format ML Output' >> beam.Map(format_as_json)
             | 'ML save' >> beam.io.WriteToText(file_path_prefix='output/sismos ML'
                                   , file_name_suffix='.json'
                                   )
        )




if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
