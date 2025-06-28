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


def main(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    # Create schema
    schema = pa.schema([
        ('UTC_Date', pa.timestamp('us')),
        ('Profundity', pa.string()),
        ('Magnitude', pa.string()),
        ('Date', pa.date32()),
        ('Hour', pa.time32('ms')),  # pa.time64('us')),
        ('Location', pa.string()),
        ('Latitude', pa.decimal128(5, 3)),
        ('Longitude', pa.decimal128(6, 3))
    ])

    # Load avro schema
    parsed_schema = fastavro.schema.load_schema('resources/Earthquake schema.avsc')

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the CSV file
        input_path = 'data/EarthquakesChile_2000-2024.csv'
        lines = (p | 'Read CSV' >> beam.io.ReadFromText(input_path, skip_header_lines=1)
                 | "Transform data" >> beam.ParDo(_OutputFn())
                 )

        # Write the data to an Avro file
        lines | 'Write to Avro' >> WriteToAvro(
            'output/file_avro1_',
            schema=parsed_schema,
            file_name_suffix='.avro',
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
