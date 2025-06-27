# Usa una imagen base de Python
FROM python:3.11-slim

# Instala dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
 && rm -rf /var/lib/apt/lists/*

# Establece directorio de trabajo
WORKDIR /app

# Copia todo el proyecto
COPY . .

# Instala dependencias de Python
RUN pip install --no-cache-dir \
    apache-airflow==2.8.2 \
    apache-beam \
    kafka-python

# Comando por defecto (puedes cambiarlo al script que quieras)
CMD ["python", "scripts/dag.py"]