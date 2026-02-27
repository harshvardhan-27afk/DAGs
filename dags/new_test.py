from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


# ------------------------
# Python Callables
# -----------------------

def choose_branch(**context):
    """
    Decide which branch to take.
    Returns task_id of the next task.
    """
    value = 10
    if value > 5:
        return "run_snowflake_task"
    else:
        return "skip_snowflake_task"


def process_data(**context):
    """
    Pull result from XCom and process.
    """
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="run_snowflake_task")
    print(f"Snowflake result: {result}")


# -----------------------
# DAG Definition
# -----------------------

with DAG(
    dag_id="branching_snowflake_example",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "branching"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    branch = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=choose_branch,
    )

    run_snowflake = SnowflakeOperator(
        task_id="run_snowflake_task",
        snowflake_conn_id="snowflake_default",
        sql="""
            SELECT CURRENT_TIMESTAMP;
        """,
        do_xcom_push=True,
    )

    skip_snowflake = EmptyOperator(
        task_id="skip_snowflake_task"
    )

    process = PythonOperator(
        task_id="process_result",
        python_callable=process_data,
        provide_context=True,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # -----------------------
    # Dependencies
    # -----------------------

    start >> branch
    branch >> run_snowflake >> process
    branch >> skip_snowflake
    [process, skip_snowflake] >> end
