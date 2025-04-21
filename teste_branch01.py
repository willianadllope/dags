from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration


@task
def downstream_function_branch_1():
    print("Upstream DAG 1 has completed. Starting tasks of branch 1.")


@task
def downstream_function_branch_2():
    print("Upstream DAG 2 has completed. Starting tasks of branch 2.")


@task
def downstream_function_branch_3():
    print("Upstream DAG 3 has completed. Starting tasks of branch 3.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(seconds=5),
}


@dag(
    start_date=datetime(2022, 8, 1),
    max_active_runs=3,
    schedule_interval=None,
    catchup=False,
)
def external_task_sensor_taskflow_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    ets_branch_1 = ExternalTaskSensor(
        task_id="ets_branch_1",
        external_dag_id="upstream_dag_1",
        external_task_id="my_task",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
    )

    task_branch_1 = downstream_function_branch_1()

    ets_branch_2 = ExternalTaskSensor(
        task_id="ets_branch_2",
        external_dag_id="upstream_dag_2",
        external_task_id="my_task",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
    )

    task_branch_2 = downstream_function_branch_2()

    ets_branch_3 = ExternalTaskSensor(
        task_id="ets_branch_3",
        external_dag_id="upstream_dag_3",
        external_task_id="my_task",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
    )

    task_branch_3 = downstream_function_branch_3()

    start >> [ets_branch_1, ets_branch_2, ets_branch_3]

    ets_branch_1 >> task_branch_1
    ets_branch_2 >> task_branch_2
    ets_branch_3 >> task_branch_3

    [task_branch_1, task_branch_2, task_branch_3] >> end


external_task_sensor_taskflow_dag()