import pyarrow.parquet as pq

def leer_archivo_parquet(nombre_archivo):
    """
    Lee un archivo Parquet e imprime su contenido.

    Args:
        nombre_archivo (str): El nombre del archivo Parquet a leer.
    """
    try:
        # Leer el archivo Parquet
        tabla_parquet = pq.read_table(nombre_archivo)
        
        # Convertir a pandas DataFrame para una visualización más amigable
        df = tabla_parquet.to_pandas()
        
        # Imprimir el contenido del DataFrame
        print(df)
        
        # Opcional: Imprimir el esquema del archivo Parquet
        print("\nEsquema del archivo Parquet:")
        print(tabla_parquet.schema)
        
    except FileNotFoundError:
        print(f"Error: No se pudo encontrar el archivo Parquet: {nombre_archivo}")
    except Exception as e:
        print(f"Ocurrió un error al leer el archivo Parquet: {e}")

if __name__ == "__main__":
    # Reemplaza 'nombre_del_archivo.parquet' con el nombre de tu archivo Parquet
    nombre_archivo_parquet = 'EarthquakesChile_2000-2024_1pct.csv.lz4.parquet' #El archivo que se genera en el ejemplo anterior
    leer_archivo_parquet(nombre_archivo_parquet)
