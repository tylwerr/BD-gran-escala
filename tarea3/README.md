# Contenedor

Tener instalado Docker Desktop y devcontainers (extensión de Visual Studio Code).
Abrir Docker Desktop.
Abrir la carpeta "tarea3" en Visual Studio Code.
En command palette de Visual Studio Code, seleccionar "Dev Containers: Reopen in Container".
Una vez abierto el container se ejecuta el build, lo que instalará todas las dependencias necesarias.

# Scripts y Dependencias
## Instalar dependencias
EL container hace automáticamente esto:
Use el script en scripts/install-dependencies.sh para instalar dependencias de requirements.txt
Use el script en scripts/install-airflow.sh para instalar Apache Airflow

## Configuraciones en scripts/airflow.cfg
Esta instalación cambia las configuraciones por defecto de Airflow, los cambios son:

- dags_folder: cambiado a la carpeta airflow/dags

# Airflow
## Run Airflow

Ejecutar:
airflow standalone

Copiar credenciales para login entregadas por Apache Airflow en la consola.

Abra el url donde se levantó Airflow e ingrese las credenciales antes copiadas para logearse.

Una vez cargados los DAGs, habilitar el DAG llamado "hrl_engagement_etl" y aplicar "Trigger DAG" para que comience la ejecución.

## Kill Airflow

Presionar CTRL+C o ejecutar:
pkill -f "airflow"

## AirFlow

<http://localhost:8080>

# Nombre del DAG

hrl_engagement_etl