import sys
import os
import shutil
from time import time
from datetime import timedelta, datetime
from sqlalchemy import create_engine
import pandas as pd

import scripts.config

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models.baseoperator import chain

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task_group, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['willian.lopes@systax.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

configs = scripts.config.configs

prod01sql = scripts.config.prod01sql


def check_necessidade_execucao():
    #return "iniciar_carga"
    return "skip_execution"

def check_carga_em_execucao():
    engine = create_engine(f"mssql+pymssql://{prod01sql['UID']}:{prod01sql['PWD']}@{prod01sql['SERVER']}:{prod01sql['PORT']}/{prod01sql['DATABASE']}")
    con = engine.connect().execution_options(stream_results=True)
    df = pd.read_sql("SELECT carga from systax_app.snowflake.vw_carga_em_andamento", con)
    carga = ''
    for index,row in df.iterrows():
        carga = row['carga'].upper()
    print("Carga:",carga)
    return carga

## full || incremental || test(nao executa nada dentro dos scripts)
configs['tipoCarga'] = check_carga_em_execucao()



with DAG(
    'dag_teste01',
    #schedule="@daily",
    schedule=None,
    default_args=default_args,
    start_date=datetime(2025, 1, 21),
    tags=['cargacsv'],
    params=configs,
    max_active_tasks=3,
    catchup=False,
) as dag:
    
    start_task = EmptyOperator(
        task_id='start',
    )

    end_task = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    skip_execution = EmptyOperator(
        task_id='skip_execution',
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=check_necessidade_execucao,
    )

    iniciar_carga = EmptyOperator(
        task_id='iniciar_carga',
    )

    ## chamar somente no incremental
    task01 = BashOperator(
        task_id="task01",
        bash_command="python "+dag.params['scripts']['task_test']+" 1, '"+dag.params['tipoCarga']+"'",
    )

    task02 = BashOperator(
        task_id="task02",
        bash_command="python "+dag.params['scripts']['task_test']+" 2, '"+dag.params['tipoCarga']+"'",
    )   

    task03 = BashOperator(
        task_id="task03",
        bash_command="python "+dag.params['scripts']['task_test']+" 3, '"+dag.params['tipoCarga']+"'",
    )   

    chain(
        start_task, 
        branching,
        iniciar_carga,
        task01,
        task02,
        task03,
        end_task
    )
    chain(
        start_task, 
        branching,
        skip_execution,
        end_task
    )

