# Imagen base oficial de Airflow
FROM apache/airflow:2.9.0

# Instalar dependencias de los DAGs
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
