from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="test_hello_world_dag",
    start_date=datetime(2025, 11, 28),
    schedule=None,
    catchup=False,
    tags=["test"],
)
def test():
    @task
    def say_hello():
        print("1")
        return "ok"

    say_hello()

my_dag_instance = test()