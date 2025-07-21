"""Example DAG demonstrating the usage of the BranchPythonOperator."""

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

import random
from pendulum import datetime


def random_branch():
    from random import randint

    return "branch_a" if randint(1, 2) == 1 else "branch_b"

with DAG(
    dag_id='teste_branch04',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule=None
) as dag:

    run_this_first = DummyOperator(
        task_id='run_this_first',
    )

    complete = DummyOperator(
        task_id="complete", 
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=random_branch,
    )

    branch_a = DummyOperator(
            task_id="branch_a",
        )
    
    branch_b = DummyOperator(
            task_id="branch_b",
    )

    do_x = DummyOperator(task_id="do_x")

    do_y = DummyOperator(task_id="do_y")

    do_z = DummyOperator(task_id="do_z")

    do_t = DummyOperator(task_id="do_t")

    run_this_first >> branching >> branch_a >> do_z >> do_t >> complete
    run_this_first >> branching >> branch_b >> do_x >> do_y >> complete
    

        