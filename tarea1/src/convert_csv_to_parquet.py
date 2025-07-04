import csv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date, time
from decimal import Decimal
from .convert import Convert

class ConvertCsvToParquet(Convert):

    def convert(self, input_filename, output_folder, compression):
        # Get output filename
        output_path = self.get_output_filename(input_filename, output_folder, "parquet", compression)
        print(f"Converting {input_filename} to Parquet with compression {compression}")

        # Create schema
        schema = pa.schema([
                ("UTC_Date", pa.timestamp("ms")),
                ("Profundity", pa.string()),
                ("Magnitude", pa.string()), 
                ("Date", pa.date32()),
                ("Hour", pa.time32("s")),
                ("Location", pa.string()),
                ("Latitude", pa.decimal128(5, 3)),
                ("Longitude", pa.decimal128(6, 3))
        ])

        # Reading CSV and converting to Parquet
        registros = []
        with open(input_filename, encoding='utf-8', newline='') as csvfile:
            datareader = csv.DictReader(csvfile, delimiter=',')
            for line in datareader:
                registros.append({
                            "UTC_Date": datetime.fromisoformat(line["UTC_Date"]),
                            "Profundity": line["Profundity"],
                            "Magnitude": line["Magnitude"],
                            "Date": date.fromisoformat(line["Date"]),
                            "Hour": time.fromisoformat(line["Hour"]),
                            "Location": line["Location"],
                            "Latitude": Decimal(line["Latitude"]),
                            "Longitude": Decimal(line["Longitude"]),
                        })

        df = pd.DataFrame(registros)
        table = pa.Table.from_pandas(df, schema=schema)
        pq.write_table(table, output_path, compression=compression)
        
        print(f"Parquet file created successfully at {output_path}")
        return output_path