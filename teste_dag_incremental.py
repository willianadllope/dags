from datetime import timedelta, datetime
import scripts.config

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models.baseoperator import chain

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task_group, task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['willian.lopes@systax.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

pastas = scripts.config.diretorios;

pastas['tipoCarga'] = 'incremental';

with DAG(
    'teste_dag_incremental',
    #schedule="@daily",
    schedule_interval=None,
    default_args=default_args,
    start_date=datetime(2025, 1, 21),
    tags=['tabelaoprod01'],
    params=pastas,
    concurrency=3,
    catchup=False,
) as dag:
    
    start_task = DummyOperator(
        task_id='start',
    )

    task_exec_incremental = BashOperator(
        task_id="task_exec_incremental",
        bash_command="echo 'task_exec_incremental'",
    )

    end_task = DummyOperator(
        task_id='end',
    )

    chain(
        start_task, 
        task_exec_incremental,
        end_task
    )


