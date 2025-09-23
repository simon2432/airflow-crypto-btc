from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

@dag(
    dag_id="sanity_hello",
    start_date=datetime(2024,1,1),
    schedule=None,              # lo lanzamos manual
    catchup=False,
    default_args={"retries": 0}
)
def sanity():
    hello = BashOperator(task_id="say_hello", bash_command="echo 'Hola Airflow!'")

    @task
    def add(a: int, b: int) -> int:
        return a + b

    s = add(2, 3)
    hello >> s

sanity()
