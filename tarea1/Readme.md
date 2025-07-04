# TODO

# Integrantes

* Benjamín Daza Jiménez
* Daniel Maturana Cristino
* Sebastián Von Kunowsky Lepe

Para la presente tarea debe completar:

1. AVRO esquema en "Earthquake schema.avsc" 
2. Parquet esquema en "convert_csv_to_parquet.py" 
3. Codigo necesario para convertir el archivo CSV a AVRO y escribir los datos en un archivo .avro usando el algoritmo de compresión pasado como parametro en "convert_csv_to_avro.py"
4. Codigo necesario para convertir el archivo CSV a Parque y escribir los datos en un archivo .parque usando el algoritmo de compresión pasado como parametro en "convert_csv_to_parquet.py"

# Esquema

- UTC_Date: Timestamp de 64 bytes
- Profundity: String
- Magnitude: String
- Date: Date de 32 bytes
- Hour: Time de 32 bytes
- Location: String
- Latitude: Decimal(precision=5, scale=3)
- Longitude: Decimal(precision=6, scale=3)


# Estructura del proyecto

```
.
├── .devcontainer -> contiene los archivos de definición del devcontainer  
│   └── devcontainer.json -> especifica la imagen Docker del devcontainer y la configuración de VS Code
├── app.py -> metodo principal
├── data -> carpeta con data raw
│   ├── EarthquakesChile_2000-2024_1pct.csv -> muestra aleatoria que representa el 1% del conjunto de datos total
│   ├── EarthquakesChile_2000-2024_10pct.csv -> muestra aleatoria que representa el 10% del conjunto de datos total
│   ├── EarthquakesChile_2000-2024_25pct.csv -> muestra aleatoria que representa el 25% del conjunto de datos total
│   ├── EarthquakesChile_2000-2024_50pct.csv -> muestra aleatoria que representa el 50% del conjunto de datos total
│   └── EarthquakesChile_2000-2024.csv -> 100% del conjunto de datos
├── output -> carpeta que almacena la salida de la aplicación
├── Readme.md -> este archivo
├── requirements.txt -> dependecias
├── resources -> recursos de la aplicación
│   └── Earthquake schema.avsc -> esquema avro usado por la aplicación
├── scripts -> carpeta con los scripts necesarios para la construcción del devcontainer
│   └── install-dependencies.sh -> script que instala dependencias necesarias para ejecutar la aplicacion en un devcontainer
└── src
    ├── convert_csv_to_avro.py -> convierte un archivo CSV a AVRO
    ├── convert_csv_to_parquet.py -> convierte un archivo CSv a Parquet
    └── convert.py -> clase padre
```

# Ejecutar conversion de archivos

Para ejecutar la conversión de los archivos, deje ejecutar el siguiente comando en la carpeta raíz del proyecto

```
python app.py 
```


# CLI tools

Para inspeccionar los archivos a través de la línea de comandos, podemos usar:

## parquet-cli       

Ver metadata
`$ parq input.parquet`

Obtener esquema
`$ parq input.parquet --schema`

Obtener total de records en archivo
`$ parq input.parquet --count`

Obtener los primeros N records
`$ parq input.parquet --head 10`

## fastavro 

Obtener esquema
`$ fastavro --schema input.avro`

Mostrar archivo 
`$ fastavro input.avro`

# Documentación

- El set de datos usado fue extraído de: https://www.kaggle.com/datasets/javierquinterosm/earthquakes-in-chile-2000-2024/data
- Tipos de datos en AVRO: https://avro.apache.org/docs/1.12.0/specification/
- Libreria AVRO: https://fastavro.readthedocs.io/en/latest/
- Definición esquema en Parquet/Arrow: https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html, https://arrow.apache.org/docs/python/api/datatypes.html
- Libreria Parquet: https://arrow.apache.org/docs/python/parquet.html#compression-encoding-and-file-compatibility
- Visor/conversor de archivos online: https://konbert.com/
