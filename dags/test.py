
# from dag_parser.dynamic.dag_context import DAG, PythonOperator
# from datetime import datetime
# from datetime import timedeltax
#iddssdxcfgbcfsss

from dag_parser.dynamic.dag_context import DAG, PythonOperator
from datetime import datetime

default_args = {
    "owner": "pi-flow",
    "retries": 0,
    "retry_delay": 60,
}

def hello_world():
    print("Hello from Pi-Flow!")

def print_result(**context):
    print("Pipeline completed successfully!")

with DAG(
    dag_id="simple_test_dag",
    start_date=datetime(2026,2,25),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["test"],
) as dag:

    task_start = PythonOperator(
        task_id="start_task",
        python_callable=hello_world,
    )

    task_end = PythonOperator(
        task_id="end_task",
        python_callable=print_result,
    )

    task_start >> task_end

