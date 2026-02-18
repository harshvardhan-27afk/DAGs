from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
import random


default_args = {
    "owner": "pi-flow",
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
}


def start():
    print("🚀 Pipeline started")


def choose_branch():
    choice = random.choice(["path_a", "path_b"])
    print(f"🔀 Branch selected: {choice}")
    return choice


def task_a():
    print("✅ Executing Path A")


def task_b():
    print("✅ Executing Path B")


def dynamic_task(task_number):
    print(f"⚙ Running dynamic task {task_number}")


def finalize():
    print("🎯 Pipeline completed")


with DAG(
    dag_id="complex_demo_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["demo", "pi-flow"],
) as dag:

    start_task = PythonOperator(
        task_id="start_task",
        python_callable=start,
    )

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=choose_branch,
    )

    path_a = PythonOperator(
        task_id="path_a",
        python_callable=task_a,
    )

    path_b = PythonOperator(
        task_id="path_b",
        python_callable=task_b,
    )

    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )

    # ✅ Task Group
    with TaskGroup("processing_group") as processing_group:

        preprocess = PythonOperator(
            task_id="preprocess",
            python_callable=lambda: print("🧹 Preprocessing data"),
        )

        dynamic_tasks = []
        for i in range(3):
            t = PythonOperator(
                task_id=f"dynamic_task_{i}",
                python_callable=dynamic_task,
                op_kwargs={"task_number": i},
            )
            dynamic_tasks.append(t)

        aggregate = PythonOperator(
            task_id="aggregate",
            python_callable=lambda: print("📊 Aggregating results"),
        )

        preprocess >> dynamic_tasks >> aggregate

    finalize_task = PythonOperator(
        task_id="finalize",
        python_callable=finalize,
    )

    # ✅ Dependencies
    start_task >> branching
    branching >> [path_a, path_b]
    path_a >> join
    path_b >> join
    join >> processing_group >> finalize_task
