import csv
import fastavro
from datetime import datetime, date, time
from decimal import Decimal
from pathlib import Path
from .convert import Convert

def decimal_to_bytes(value: Decimal, scale: int, precision: int) -> bytes:
    """
    Convierte un Decimal a bytes, de acuerdo al tipo logicalType 'decimal' de Avro.
    """
    # Escalar el valor decimal (por ejemplo: 12.345 con scale=3 -> 12345)
    scaled = int((value * (10 ** scale)).to_integral_value())
    
    # Calcular cuántos bytes se necesitan para representar el número
    num_bytes = (scaled.bit_length() + 8) // 8 or 1
    
    return scaled.to_bytes(num_bytes, byteorder='big', signed=True)



class ConvertCsvToAvro(Convert):

    def convert(self, input_filename, output_folder, compression):
        # Get output filename
        output_path = self.get_output_filename(input_filename, output_folder, "avro", compression)
        print(f"Converting {input_filename} to AVRO with compression {compression}")
        
        # Load avro schema
        parsed_schema = fastavro.schema.load_schema('resources/Earthquake schema.avsc')

        # Reading CSV and converting to AVRO
        registros = []
        with open(input_filename, encoding='utf-8', newline='') as csvfile:
            datareader = csv.DictReader(csvfile, delimiter=',')
            for line in datareader:
                 hora = time.fromisoformat(line["Hour"])

                 registros.append({
                            "UTC_Date": int(datetime.fromisoformat(line["UTC_Date"]).timestamp() * 1000),
                            "Profundity": line["Profundity"],
                            "Magnitude": line["Magnitude"],
                            "Date": (date.fromisoformat(line["Date"]) - date(1970, 1, 1)).days,
                            "Hour": (hora.hour * 3600 + hora.minute * 60 + hora.second) * 1000,
                            "Location": line["Location"],                           
                            "Latitude": decimal_to_bytes(Decimal(line["Latitude"]), scale=3, precision=5),
                            "Longitude": decimal_to_bytes(Decimal(line["Longitude"]), scale=3, precision=6)
                        })

        
        with open(output_path, 'wb') as out_avro:
            fastavro.writer(out_avro, parsed_schema, registros, codec=compression)


        print(f"AVRO file created successfully at {output_path}")
        return output_path
            