# Consideración
Para que todo funcione la carpeta output debe estar vacía.

Debe asegurarse de que en los archivos .sh su Visual Studio Code tenga marcado el Select End of Line Sequence como "LF" y no "CRLF".

# Contenedor

1. Tener instalado Docker Desktop y devcontainers (extensión de Visual Studio Code).
2. Abrir Docker Desktop.
3. Abrir la carpeta "proyecto" en Visual Studio Code.
4. En command palette de Visual Studio Code, seleccionar "Dev Containers: Reopen in Container".
5. Una vez abierto el container se ejecuta el build, lo que instalará todas las dependencias necesarias que se detallan abajo.

# Scripts y Dependencias
## Instalar dependencias
EL container hace automáticamente esto:

- Use el script en scripts/install-dependencies.sh para instalar dependencias de requirements.txt
- Use el script en scripts/install-airflow.sh para instalar Apache Airflow

## Configuraciones en scripts/airflow.cfg
Esta instalación cambia las configuraciones por defecto de Airflow, los cambios son:

- dags_folder: cambiado a la carpeta /proyecto/dags

# Airflow
## Run Airflow

1. Ejecutar: airflow standalone
2. Copiar credenciales para login entregadas por Apache Airflow en la consola.
3. Abra el url donde se levantó Airflow e ingrese las credenciales antes copiadas para logearse.
4. Una vez cargados los DAGs, habilitar el DAG llamado "etl_jsonl_to_parquet" y aplicar "Trigger DAG" para que comience la ejecución de las tareas.

## Kill Airflow

Presionar CTRL+C o ejecutar: pkill -f "airflow"

## AirFlow

<http://localhost:8080>

## Nombre del DAG

etl_jsonl_to_parquet

# Kafka
Primero, asegurarse de que el usuario que ejecuta el script tenga permisos de escritura en el directorio. Considerar que este proceso no se detiene, ya que está constantemente leyendo los mensajes de Kafka.

Para ejecutar el proceso streaming en etl_streaming.py, ejecutar esto en la consola del container reemplazando las variables por lo requerido a su caso de uso:

python src/etl_streaming.py --bootstrap_servers localhost:9099 --topic sensor_data --output_path /path/to/your/output

- localhost:9099: Sustituye con la dirección y puerto del broker de Kafka.
- sensor_data: Sustituye por el nombre del tópico de Kafka.
- /path/to/your/output: Sustituye por la ruta donde Beam escribirá los archivos Parquet.
