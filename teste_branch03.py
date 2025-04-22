"""Example DAG demonstrating the usage of the ShortCircuitOperator."""

from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from pendulum import datetime

with DAG(
    dag_id='short_circuit_operator_example',
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    run_this_first = EmptyOperator(
        task_id='run_this_first',
    )

    complete_true = DummyOperator(
        task_id="complete_true", 
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    complete_false = DummyOperator(
        task_id="complete_false", 
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    cond_true = ShortCircuitOperator(
        task_id='condition_is_True',
        python_callable=lambda: True,
    )

    cond_false = ShortCircuitOperator(
        task_id='condition_is_False',
        python_callable=lambda: True,
    )

    ds_true = EmptyOperator(task_id='ds_true')
    ds_false = EmptyOperator(task_id='ds_false')

    run_this_first >> cond_true >> ds_true >> complete_true
    run_this_first >> cond_false >> ds_false >> complete_false
