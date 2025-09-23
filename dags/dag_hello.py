from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def _say_hello():
    print("hola airflow")

with DAG(
    dag_id="dag_hello",
    description="DAG mínimo de prueba: imprime 'hola airflow'",
    start_date=datetime(2025, 9, 1),
    schedule="@once",          # se ejecuta una vez cuando lo actives
    catchup=False,             # no intenta correr fechas históricas
    tags=["test", "hello"]
) as dag:
    hello = PythonOperator(
        task_id="say_hello",
        python_callable=_say_hello
    )
