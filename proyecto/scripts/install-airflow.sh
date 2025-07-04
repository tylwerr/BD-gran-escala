#!/usr/bin/env bash
set -e

# Airflow version
AIRFLOW_VERSION=2.11.0

# Detect Python version (ej: 3.12)
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

# Constraint URL oficial
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "Instalando Airflow $AIRFLOW_VERSION con restricciones para Python $PYTHON_VERSION..."
python -m pip install --upgrade pip

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install apache-airflow-providers-apache-beam==6.1.0
pip install confluent-kafka
pip install virtualenv

echo "Airflow instalado."

# Inicializar Airflow
echo "Inicializando base de datos de Airflow..."
airflow db init

# (Opcional) Copiar configuración si tienes archivos personalizados
if [[ -f scripts/airflow.cfg ]]; then
  echo "Copiando configuración personalizada..."
  mkdir -p ~/airflow
  cp scripts/airflow.cfg ~/airflow/airflow.cfg
fi

# Crear DAGs folder si no existe
mkdir -p ~/dags

echo "Configuración lista. Puedes correr ahora:"
echo "airflow standalone"
