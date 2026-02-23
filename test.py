from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
#commentdd
default_args = {
    "owner": "pi-flow",
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

def hello_world():
    print("Hello from Pi-Flow!")

def print_result(**context):
    print("Pipeline completed successfully!")

with DAG(
    dag_id="simple_test_dag",
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["test"],
) as dag:

    task_start = PythonOperator(
        task_id="hello_task",
        python_callable=hello_world,
    )



    task_start 
