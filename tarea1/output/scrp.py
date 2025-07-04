import fastavro
from decimal import Decimal

def bytes_to_decimal(value_bytes: bytes, scale: int) -> Decimal:
    """
    Decodifica un valor decimal guardado como bytes, de acuerdo al esquema Avro.
    """
    unscaled = int.from_bytes(value_bytes, byteorder='big', signed=True)
    return Decimal(unscaled) / (10 ** scale)

# Leer el archivo Avro
with open("EarthquakesChile_2000-2024_1pct.csv.null.avro", 'rb') as f:
    reader = fastavro.reader(f)
    for record in reader:
        latitude = record["Latitude"]
        longitude = record["Longitude"]

        
        # Mostrar todos los campos
        print({
            "UTC_Date": record["UTC_Date"],
            "Profundity": record["Profundity"],
            "Magnitude": record["Magnitude"],
            "Date": record["Date"],
            "Hour": record["Hour"],
            "Location": record["Location"],
            "Latitude": latitude,
            "Longitude": longitude
        })
