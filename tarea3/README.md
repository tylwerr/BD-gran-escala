# Contenedor

1. Tener instalado Docker Desktop y devcontainers (extensión de Visual Studio Code).
2. Abrir Docker Desktop.
3. Abrir la carpeta "tarea3" en Visual Studio Code.
4. En command palette de Visual Studio Code, seleccionar "Dev Containers: Reopen in Container".
5. Una vez abierto el container se ejecuta el build, lo que instalará todas las dependencias necesarias que se detallan abajo.

# Scripts y Dependencias
## Instalar dependencias
EL container hace automáticamente esto:

- Use el script en scripts/install-dependencies.sh para instalar dependencias de requirements.txt
- Use el script en scripts/install-airflow.sh para instalar Apache Airflow

## Configuraciones en scripts/airflow.cfg
Esta instalación cambia las configuraciones por defecto de Airflow, los cambios son:

- dags_folder: cambiado a la carpeta airflow/dags

# Airflow
## Run Airflow

1. Ejecutar: airflow standalone
2. Copiar credenciales para login entregadas por Apache Airflow en la consola.
3. Abra el url donde se levantó Airflow e ingrese las credenciales antes copiadas para logearse.
4. Una vez cargados los DAGs, habilitar el DAG llamado "hrl_engagement_etl" y aplicar "Trigger DAG" para que comience la ejecución de las tareas.

## Kill Airflow

Presionar CTRL+C o ejecutar: pkill -f "airflow"

## AirFlow

<http://localhost:8080>

## Nombre del DAG

hrl_engagement_etl
