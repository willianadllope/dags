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

from airflow.operators import trigger_dagrun
import teste_dag_incremental
import teste_dag_full

dag_incremental = DAG('teste_dag_incremental')
dag_full = DAG('teste_dag_full')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['willian.lopes@systax.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

pastas = scripts.config.pastas;

pastas['tipoCarga'] = 'incremental';

with DAG(
    'teste_dag_inicial',
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

    task_check_execucao = BashOperator(
        task_id="task_check_execucao",
        bash_command="echo 'task_check_execucao'",
    )

    @task()
    def inicia_carga_full():
        trigger = TriggerDagRunOperator(
            task_id='trigger_data_processing',
            trigger_dag_id='teste_dag_full',
            dag=dag_full
        )
        return trigger

    task_carga_incremental = BashOperator(
        task_id="task_carga_incremental",
        bash_command="echo 'task_carga_incremental'",
    )

    end_task = DummyOperator(
        task_id='end',
    )

    task_carga_full = inicia_carga_full();
    
    chain(
        start_task, 
        task_check_execucao,
        task_carga_full,
        end_task
    )


